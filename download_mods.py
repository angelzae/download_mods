import os
import time
import shutil
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from prompt_toolkit import prompt
from prompt_toolkit.shortcuts import radiolist_dialog, input_dialog, message_dialog, button_dialog

# --- 全局配置字典 ---
CONFIG = {
    'MAX_WORKERS_NORMAL': 10,
    'MAX_WORKERS_WHEN_LARGE_FILES_EXIST': 10,
    'LARGE_FILE_THRESHOLD': 20 * 1024 * 1024,
    'THREADS_PER_LARGE_FILE': 10,
    'CHUNK_SIZE_FOR_LARGE_FILES': 30 * 1024 * 1024,
    'SAVE_DIR': None,
    'MAX_RETRIES': 3,
    'RETRY_DELAY': 5
}

# --- 请求头 ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Encoding': 'identity'
}


# --- 核心下载功能 ---

def discover_files_recursive(url, base_url, local_path, file_list, visited_urls, added_files_set):
    """
    递归扫描网站目录，找出所有需要下载的文件。
    """
    if url in visited_urls:
        return
    visited_urls.add(url)
    try:
        response = requests.get(url, timeout=30, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all('a'):
            href = link.get('href')
            if not href or href.startswith('?') or href == '../':
                continue
            full_url = urljoin(url, href)
            if not full_url.startswith(base_url):
                continue
            decoded_href = unquote(href)
            if href.endswith('/'):
                new_local_path = os.path.join(local_path, decoded_href.strip('/'))
                discover_files_recursive(full_url, base_url, new_local_path, file_list, visited_urls, added_files_set)
            else:
                file_path = os.path.join(local_path, decoded_href)
                if file_path not in added_files_set:
                    file_list.append((full_url, file_path))
                    added_files_set.add(file_path)
    except requests.exceptions.RequestException as e:
        print(f"!! 错误：扫描URL失败 {url} - {e}")


def download_chunk(args):
    """
    下载单个文件分块。
    """
    url, chunk_path, start_byte, end_byte, relative_path, chunk_num, total_chunks = args
    headers = {**HEADERS, 'Range': f'bytes={start_byte}-{end_byte}'}
    chunk_label = f"{relative_path} (块 {chunk_num + 1}/{total_chunks})"
    for attempt in range(CONFIG['MAX_RETRIES']):
        try:
            print(f"[分块下载] -> {chunk_label} (尝试 {attempt + 1}/{CONFIG['MAX_RETRIES']})")
            with requests.get(url, headers=headers, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(chunk_path, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            return chunk_path
        except requests.exceptions.RequestException as e:
            print(f"!! [分块失败] {chunk_label} - {e}")
            if attempt + 1 < CONFIG['MAX_RETRIES']:
                time.sleep(CONFIG['RETRY_DELAY'])
            else:
                print(f"!! [分块放弃] {chunk_label} 在 {CONFIG['MAX_RETRIES']}次尝试后彻底失败。")
                return None


def download_large_file(url, local_path, file_size, relative_path):
    """
    使用多线程分块下载大文件。
    返回 True 表示成功, False 表示失败。
    """
    threads_per_file = CONFIG['THREADS_PER_LARGE_FILE']
    chunk_size = CONFIG['CHUNK_SIZE_FOR_LARGE_FILES']
    temp_dir = local_path + "_chunks"
    os.makedirs(temp_dir, exist_ok=True)

    try:
        total_chunks = (file_size + chunk_size - 1) // chunk_size
        tasks = []
        for i in range(total_chunks):
            start = i * chunk_size
            end = min(start + chunk_size - 1, file_size - 1)
            chunk_path = os.path.join(temp_dir, f"chunk_{i}")
            tasks.append((url, chunk_path, start, end, relative_path, i, total_chunks))

        successful_chunks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads_per_file) as executor:
            results = executor.map(download_chunk, tasks)
            successful_chunks = [result for result in results if result is not None]

        if len(successful_chunks) != total_chunks:
            print(f"!! [下载失败] 文件 {relative_path} 因部分分块下载失败而中止。")
            if os.path.exists(local_path): os.remove(local_path)
            return False

        print(f"[合并中] -> {relative_path} (共 {total_chunks} 个分块)")
        try:
            with open(local_path, 'wb') as final_file:
                for i in range(total_chunks):
                    chunk_path = os.path.join(temp_dir, f"chunk_{i}")
                    with open(chunk_path, 'rb') as chunk_file:
                        shutil.copyfileobj(chunk_file, final_file)
            print(f"[成功] -> {relative_path}")
            return True
        except Exception as e:
            print(f"!! [合并错误] {relative_path} - {e}")
            if os.path.exists(local_path): os.remove(local_path)
            return False
    finally:
        if os.path.isdir(temp_dir):
            print(f"[清理中] -> 正在删除临时分块目录: {os.path.basename(temp_dir)}")
            start_time = time.time()
            shutil.rmtree(temp_dir)
            end_time = time.time()
            print(f"[清理完成] -> 删除耗时: {end_time - start_time:.2f} 秒。")


def download_file_normally(response, local_path, relative_path):
    """
    标准单线程下载文件。
    返回 True 表示成功, False 表示失败。
    """
    try:
        print(f"[下载中] -> {relative_path}")
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[成功] -> {relative_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"!! [失败] {relative_path} - {e}")
        return False


def download_dispatcher(file_info_with_savedir):
    """
    下载调度器。
    返回 None 表示成功或跳过，返回 url 表示失败。
    """
    url, local_path, save_dir = file_info_with_savedir
    relative_path = os.path.relpath(local_path, os.path.dirname(save_dir))
    if os.path.exists(local_path):
        print(f"[跳过] 文件已存在: {relative_path}")
        return None

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        with requests.get(url, stream=True, timeout=30, allow_redirects=True, headers=HEADERS) as r:
            r.raise_for_status()
            content_length = r.headers.get('Content-Length')
            accept_ranges = r.headers.get('Accept-Ranges')
            size_mb = int(content_length) / 1024 / 1024 if content_length and content_length.isdigit() else 0
            support_range = "是" if accept_ranges == 'bytes' else "否"

            print(f"[检查] -> {relative_path} | 大小: {size_mb:.2f} MB | 支持分块: {support_range}")

            # 决策逻辑
            if (content_length and content_length.isdigit() and
                    accept_ranges == 'bytes' and
                    int(content_length) > CONFIG['LARGE_FILE_THRESHOLD']):
                print(f"-> [决策] 大文件，启动 {CONFIG['THREADS_PER_LARGE_FILE']} 线程分块下载...")
                r.close()
                success = download_large_file(url, local_path, int(content_length), relative_path)
                return None if success else url
            else:
                print("-> [决策] 小文件或不支持分块，继续标准下载...")
                success = download_file_normally(r, local_path, relative_path)
                return None if success else url
    except requests.exceptions.RequestException as e:
        print(f"!! [检查失败] {relative_path} ({e})。无法下载。")
        return url


# --- UI和主流程函数 ---

def set_config_value(key, title, validator=int, unit=''):
    """
    通用设置项修改函数
    """
    current_value = CONFIG[key]
    if key == 'LARGE_FILE_THRESHOLD':
        current_display_value = f"{current_value / 1024 / 1024:.2f} MB"
    else:
        current_display_value = f"{current_value}{unit}"

    new_value_str = input_dialog(
        title=f"设置 - {title}",
        text=f"请输入新的值 (当前: {current_display_value}):"
    ).run()

    if new_value_str:
        try:
            new_value = validator(new_value_str)
            if key == 'LARGE_FILE_THRESHOLD':
                CONFIG[key] = int(new_value * 1024 * 1024)
                final_display = f"{new_value} MB"
            else:
                CONFIG[key] = new_value
                final_display = f"{new_value}{unit}"
            message_dialog(title="成功", text=f"{title} 已更新为: {final_display}").run()
        except (ValueError, TypeError):
            message_dialog(title="错误", text="输入无效，请输入一个正确的数值。").run()


def set_save_location():
    """设置下载位置的函数"""
    current_path = CONFIG['SAVE_DIR'] if CONFIG['SAVE_DIR'] else "未设置 (将自动创建)"
    new_path = input_dialog(
        title="设置下载位置",
        text=f"请输入新的本地文件夹路径 (当前: {current_path}):"
    ).run()
    if new_path:
        CONFIG['SAVE_DIR'] = new_path
        message_dialog(title="成功", text=f"下载位置已更新为: {new_path}").run()


def show_settings_menu():
    """显示设置菜单，并根据用户选择调用相应的设置函数。"""
    while True:
        setting_options = [
            ('LARGE_FILE_THRESHOLD', f"大文件阈值 (当前: {CONFIG['LARGE_FILE_THRESHOLD'] / 1024 / 1024:.2f} MB)"),
            ('THREADS_PER_LARGE_FILE', f"单文件多线程数 (当前: {CONFIG['THREADS_PER_LARGE_FILE']})"),
            ('MAX_WORKERS_NORMAL', f"小文件最大并行任务数 (当前: {CONFIG['MAX_WORKERS_NORMAL']})"),
            ('MAX_WORKERS_WHEN_LARGE_FILES_EXIST',
             f"大文件模式最大并行任务数 (当前: {CONFIG['MAX_WORKERS_WHEN_LARGE_FILES_EXIST']})"),
            ('SAVE_DIR', f"下载位置 (当前: {CONFIG['SAVE_DIR'] or '自动'})"),
            ('back', '返回主菜单')
        ]

        selected_option = radiolist_dialog(
            title="设置菜单",
            text="请使用方向键选择要修改的配置项，按Enter确认。",
            values=setting_options
        ).run()

        if selected_option is None or selected_option == 'back':
            break
        elif selected_option == 'LARGE_FILE_THRESHOLD':
            set_config_value(selected_option, "大文件阈值", float, " MB")
        elif selected_option == 'THREADS_PER_LARGE_FILE':
            set_config_value(selected_option, "单文件多线程数")
        elif selected_option == 'MAX_WORKERS_NORMAL':
            set_config_value(selected_option, "小文件最大并行任务数")
        elif selected_option == 'MAX_WORKERS_WHEN_LARGE_FILES_EXIST':
            set_config_value(selected_option, "大文件模式最大并行任务数")
        elif selected_option == 'SAVE_DIR':
            set_save_location()


def start_download_process():
    """
    封装了从获取URL到完成下载的整个核心流程。
    """
    base_url = input_dialog(
        title="输入下载链接",
        text="请输入要下载的目录URL (请直接粘贴网址并按回车):"
    ).run()

    if not base_url:
        message_dialog(title="提示", text="操作已取消。").run()
        return

    base_url = base_url.strip()
    if not base_url.endswith('/'):
        base_url += '/'

    if CONFIG['SAVE_DIR']:
        save_dir = CONFIG['SAVE_DIR']
    else:
        save_dir = unquote(base_url.strip('/').split('/')[-1])

    os.makedirs(save_dir, exist_ok=True)

    print("-" * 30)
    print(f"目标URL: {base_url}")
    print(f"文件将保存到文件夹: '{save_dir}'")
    print("-" * 30)

    print("\n--- 开始扫描文件列表... ---")
    initial_files = []
    discover_files_recursive(base_url, base_url, save_dir, initial_files, set(), set())
    print(f"\n--- 扫描完成！共发现 {len(initial_files)} 个文件。 ---")

    if not initial_files:
        print("\n--- 没有发现需要下载的文件。 ---")
        return

    # --- 新增：用于重试逻辑 ---
    file_map = {url: path for url, path in initial_files}
    urls_to_process = list(file_map.keys())

    while urls_to_process:
        print("\n--- 开始预检查文件大小以确定下载策略... ---")
        large_files_found = False
        for url in urls_to_process:
            local_path = file_map[url]
            if os.path.exists(local_path):
                continue
            try:
                with requests.head(url, timeout=10, allow_redirects=True, headers=HEADERS) as r:
                    content_length = r.headers.get('Content-Length')
                    if content_length and content_length.isdigit() and int(content_length) > CONFIG[
                        'LARGE_FILE_THRESHOLD']:
                        large_files_found = True
                        print(f"[预检查] 发现大文件: {os.path.basename(local_path)}")
                        break
            except requests.exceptions.RequestException as e:
                print(f"[预检查警告] 无法检查文件 {os.path.basename(local_path)}: {e}")
                continue

        if large_files_found:
            active_workers = CONFIG['MAX_WORKERS_WHEN_LARGE_FILES_EXIST']
            print(f"\n--- 检测到大文件，并行任务数将限制为: {active_workers} ---")
        else:
            active_workers = CONFIG['MAX_WORKERS_NORMAL']
            print(f"\n--- 未发现大文件或预检查失败，将使用常规并行数: {active_workers} ---")

        tasks_with_savedir = [(url, file_map[url], save_dir) for url in urls_to_process]

        print(f"\n--- 开始下载 (使用 {active_workers} 个主任务线程)... ---")

        failed_urls = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=active_workers) as executor:
            results = executor.map(download_dispatcher, tasks_with_savedir)
            # 过滤出失败的URL（结果不为None的项）
            failed_urls = [result for result in results if result is not None]

        # --- 新增：检查失败并询问重试 ---
        if failed_urls:
            print("\n" + "=" * 20 + " 下载失败列表 " + "=" * 20)
            for url in failed_urls:
                print(f" - {unquote(os.path.basename(url))}")
            print("=" * 55)

            should_retry = button_dialog(
                title="下载失败",
                text=f"{len(failed_urls)} 个文件下载失败。您想重试吗？",
                buttons=[("重试", True), ("取消", False)]
            ).run()

            if should_retry:
                urls_to_process = failed_urls  # 更新待处理列表为失败的URL
            else:
                print("\n--- 用户取消重试。部分文件下载失败。 ---")
                break  # 退出 while 循环
        else:
            print("\n--- 本轮所有任务均已成功！ ---")
            break  # 退出 while 循环

    print("\n--- 所有下载任务已处理完毕！ ---")
    button_dialog(title="完成", text="所有下载任务已处理完毕！", buttons=[("确定", None)]).run()


def main():
    """
    主函数，显示主菜单并循环处理用户输入。
    """
    print("--- MOD批量下载脚本 v13 (支持失败重试) ---")
    while True:
        choice = radiolist_dialog(
            title="主菜单",
            text="欢迎使用批量下载脚本，请选择操作：",
            values=[
                ("start", "开始下载 (输入链接)"),
                ("settings", "设置"),
                ("exit", "退出")
            ]
        ).run()

        if choice == "start":
            start_download_process()
        elif choice == "settings":
            show_settings_menu()
        elif choice == "exit" or choice is None:
            break


if __name__ == "__main__":
    main()