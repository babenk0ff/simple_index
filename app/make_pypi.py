import asyncio
import os
import re
import shutil
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor

import aiofiles.os
from jinja2 import Environment, FileSystemLoader

from config import config
from logger import logger


def normalize(name):
    return re.sub(r'[-_.]+', '-', name).lower()


async def prepare_output_dir(event, output_dir):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1) as pool:
        path_exists = await aiofiles.os.path.exists(output_dir)
        if path_exists:
            loop.run_in_executor(pool, shutil.rmtree, output_dir)
            loop.run_in_executor(pool, os.mkdir, output_dir)
        else:
            loop.run_in_executor(pool, os.mkdir, output_dir)
    event.set()


def get_packages(wheels_dir):
    packages = defaultdict(list)

    for file in os.listdir(wheels_dir):
        if not file.rsplit('.', maxsplit=1)[-1] == 'whl':
            continue

        name = file.split('-', maxsplit=1)[0]
        normalized_name = normalize(name)
        packages[normalized_name].append(file)

    return packages


async def get_packages_queue(packages):
    queue = asyncio.Queue()
    [await queue.put({package: files}) for package, files in packages.items()]

    return queue


async def make_main_index(event, packages, output_dir, env):
    await event.wait()

    if event.is_set():
        index_file = os.path.join(output_dir, 'index.html')

        async with aiofiles.open(index_file, 'w', encoding='utf-8') as f:
            template = env.get_template('index.html')
            context = {
                'packages': packages,
                'repo_addr': config['REPO_ADDR'],
            }
            rendered_html = await asyncio.create_task(
                template.render_async(**context),
            )
            await f.write(rendered_html)


async def make_package_index(env, package_dir, package, files):
    package_index = os.path.join(package_dir, 'index.html')

    async with aiofiles.open(
            file=package_index,
            mode='w',
            encoding='utf-8',
    ) as file:
        template = env.get_template('package_index.html')
        context = {
            'package': package,
            'files': files,
            'repo_addr': config['REPO_ADDR'],
        }
        rendered_html = await asyncio.create_task(
            template.render_async(**context),
        )
        await file.write(rendered_html)


async def make_package_dir(package_dir, files):
    exists = await aiofiles.os.path.exists(package_dir)
    if not exists:
        await aiofiles.os.mkdir(package_dir)

    for file in files:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            file_path = os.path.join(config['WHEELS_DIR'], file)
            await loop.run_in_executor(
                pool, shutil.copy2, file_path, package_dir
            )


async def handle_package(event, queue, output_dir, env):
    await event.wait()

    if event.is_set():
        while True:
            package = await queue.get()

            for package, files in package.items():
                package_dir = os.path.join(output_dir, package)
                await make_package_dir(package_dir, files)
                await make_package_index(env, package_dir, package, files)
                logger.info(f'Package processed: {package}')

            queue.task_done()


def cancel_tasks(tasks):
    [task.cancel() for task in tasks]


async def main():
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    env = Environment(
        loader=FileSystemLoader(templates_dir),
        trim_blocks=True,
        lstrip_blocks=True,
        enable_async=True,
    )

    output_dir = os.path.join(
        os.path.dirname(__file__), config['OUTPUT_DIR_NAME']
    )
    dir_ready_event = asyncio.Event()

    await asyncio.create_task(prepare_output_dir(dir_ready_event, output_dir))

    packages = get_packages(config['WHEELS_DIR'])
    packages_count = len(packages)
    packages_queue = await get_packages_queue(packages)

    await asyncio.create_task(make_main_index(
        dir_ready_event, packages, output_dir, env)
    )

    package_handlers = [
        asyncio.create_task(handle_package(
            dir_ready_event, packages_queue, output_dir, env
        ))
        for _ in range(10)
    ]
    await packages_queue.join()
    cancel_tasks(package_handlers)

    if packages_queue.empty():
        print(f'\nAll ({packages_count}) packages were processed successfully')


if __name__ == '__main__':
    asyncio.run(main())
