# 媒体下载器 (MediaDownloader)

一个功能强大、设计优雅的媒体文件下载器，专为 bricks 框架设计，支持断点续传、并发下载、流式下载和进度显示。

## 特性

✅ **断点续传** - 下载中断后可继续，避免重新下载  
✅ **并发下载** - 多线程分块下载，充分利用带宽  
✅ **流式下载** - 通过 $options.stream 参数控制，边下载边写入磁盘，内存占用低  
✅ **进度显示** - 实时显示下载进度、速度和预计时间  
✅ **跳过已下载** - 自动检测已存在的文件  
✅ **集成友好** - 完美集成到爬虫框架中  
✅ **灵活配置** - 支持自定义请求头、代理、超时等  
✅ **批量下载** - 支持多文件并发批量下载  
✅ **默认 cffi** - 默认使用 cffi 下载器，TLS 指纹与浏览器一致

## 快速开始

### 基础用法

```python
from bricks import Request
from bricks.utils.media_downloader import MediaDownloader

# 创建下载器（默认使用 cffi 下载器 + stream 模式）
downloader = MediaDownloader()

# 简单下载
request = Request(url="https://example.com/video.mp4")
downloader.download(
    request=request,
    save_path="./downloads"
)
```

### 带进度显示

```python
def progress_callback(downloaded, total, speed):
    percent = (downloaded / total) * 100
    print(f"进度: {percent:.1f}% 速度: {speed:.2f}MB/s")

request = Request(url="https://example.com/large_file.zip")
downloader.download(
    request=request,
    save_path="./downloads",
    progress_callback=progress_callback
)
```

### 高级配置

```python
from bricks import Request
from bricks.utils.media_downloader import DownloadTask

request = Request(
    url="https://example.com/video.mp4",
    headers={
        "User-Agent": "MyApp/1.0",
        "Referer": "https://example.com"
    }
)

task = DownloadTask(
    request=request,
    save_path="./downloads",
    filename="my_video.mp4",
    chunk_size=5 * 1024 * 1024,  # 5MB 分块
    max_workers=8,                # 8线程并发
    resume=True,                  # 支持断点续传
    skip_existing=True,           # 跳过已存在文件
    progress_callback=progress_callback
)

downloader.download_task(task)
```

## 集成到爬虫

### 方式 1: 在 Pipeline 中使用

```python
from bricks.spider import template
from bricks.utils.media_downloader import MediaDownloader

class MySpider(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 复用爬虫的下载器配置
        self.media_downloader = MediaDownloader(downloader=self.downloader)

    def download_media(self, context: template.Context):
        """在 pipeline 中下载媒体"""
        media_url = context.items.get('media_url')

        if media_url:
            # 构建请求对象，复用爬虫的配置
            request = Request(
                url=media_url,
                headers=context.request.headers if context.request else {},
                proxies=context.request.proxies if context.request else None
            )

            success = self.media_downloader.download(
                request=request,
                save_path="./downloads",
                progress_callback=lambda d, t, s:
                    self.logger.info(f"下载: {d}/{t}")
            )

            context.items['downloaded'] = success

        return context
```

### 方式 2: 完整爬虫示例

```python
from bricks import Request, const
from bricks.spider import template
from bricks.utils.media_downloader import MediaDownloader

class MediaSpider(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.media_downloader = MediaDownloader(downloader=self.downloader)

    @property
    def config(self):
        return template.Config(
            init=[
                template.Init(func=lambda: {"page": 1})
            ],
            download=[
                template.Download(
                    url="https://api.example.com/media/list",
                    params={"page": "{page}"},
                    headers={"User-Agent": "@chrome"}
                )
            ],
            parse=[
                template.Parse(func=self.parse_media_list)
            ],
            pipeline=[
                template.Pipeline(func=self.download_media, success=True)
            ]
        )

    def parse_media_list(self, context):
        # 解析媒体列表
        media_list = context.response.json().get('data', [])
        context.items = media_list
        return context

    def download_media(self, context):
        # 下载所有媒体
        for item in context.items:
            request = Request(
                url=item['url'],
                headers=context.request.headers,
                proxies=context.request.proxies
            )

            self.media_downloader.download(
                request=request,
                save_path="./downloads",
                filename=item['filename']
            )
        return context
```

## 批量下载

```python
from bricks import Request
from bricks.utils.media_downloader import MediaDownloader, DownloadTask

downloader = MediaDownloader()

# 定义多个下载任务
tasks = [
    DownloadTask(
        request=Request(url="https://example.com/file1.jpg"),
        save_path="./downloads"
    ),
    DownloadTask(
        request=Request(url="https://example.com/file2.mp4"),
        save_path="./downloads"
    ),
    DownloadTask(
        request=Request(url="https://example.com/file3.pdf"),
        save_path="./downloads"
    )
]

# 批量下载，最多同时3个任务
results = downloader.batch_download(tasks, max_workers=3)

for task, success in results.items():
    print(f"{task}: {'成功' if success else '失败'}")
```

## API 参考

### MediaDownloader

#### `__init__(downloader=None)`

初始化媒体下载器

- `downloader`: 下载器实例，默认使用 `CffiDownloader`（支持 stream 模式）

#### `download(request, save_path, **options)`

下载单个文件

**参数:**

- `request` (Request): 请求对象，包含 URL、请求头等配置
- `save_path` (str): 保存目录
- `filename` (str, optional): 文件名，默认从 URL 解析
- `chunk_size` (int): 分块大小，默认 1MB
- `max_workers` (int): 最大并发数，默认 1
- `resume` (bool): 是否支持断点续传，默认 True
- `skip_existing` (bool): 是否跳过已存在文件，默认 True
- `progress_callback` (callable, optional): 进度回调函数 `(downloaded, total, speed)`

**返回:** bool - 是否下载成功

#### `download_task(task)`

执行下载任务

- `task` (DownloadTask): 下载任务对象

**返回:** bool - 是否下载成功

#### `batch_download(tasks, max_workers=3, stop_on_error=False)`

批量下载

**参数:**

- `tasks` (list): 下载任务列表
- `max_workers` (int): 最大并发下载数，默认 3
- `stop_on_error` (bool): 遇到错误是否停止，默认 False

**返回:** dict - 每个任务的下载结果

### DownloadTask

下载任务配置

```python
from bricks import Request
from bricks.utils.media_downloader import DownloadTask

request = Request(
    url="...",                    # 必需：下载地址
    headers={...},                # 可选：请求头
    proxies={...},                # 可选：代理
    timeout=60                    # 可选：超时
)

task = DownloadTask(
    request=request,             # 必需：Request 对象
    save_path="...",              # 必需：保存目录
    filename=None,                # 可选：文件名
    chunk_size=1024*1024,        # 分块大小（字节）
    max_workers=1,               # 最大并发线程数
    resume=True,                 # 是否支持断点续传
    skip_existing=True,          # 是否跳过已存在文件
    progress_callback=None       # 进度回调函数
)
```

## 进度回调

进度回调函数接收三个参数：

```python
def progress_callback(downloaded, total, speed):
    """
    Args:
        downloaded (int): 已下载字节数
        total (int): 总字节数
        speed (float): 下载速度（MB/s）
    """
    if total > 0:
        percent = (downloaded / total) * 100
        print(f"进度: {percent:.1f}%")
```

## 断点续传原理

1. 下载时自动保存元数据到 `.meta` 文件
2. 下载中断后再次执行，自动读取元数据
3. 验证远程文件是否改变（ETag/Last-Modified）
4. 继续未完成的分块下载
5. 所有分块完成后合并为最终文件

## 性能优化建议

1. **大文件**: 增加 `chunk_size` 和 `max_workers`

   ```python
   chunk_size=5*1024*1024,  # 5MB
   max_workers=8
   ```

2. **小文件**: 减少线程数或使用单线程

   ```python
   max_workers=1  # 单线程
   ```

3. **网络不稳定**: 启用断点续传

   ```python
   resume=True
   ```

4. **批量下载**: 控制并发数避免过载
   ```python
   batch_download(tasks, max_workers=3)
   ```

## 注意事项

1. 确保有足够的磁盘空间
2. 并发数不宜过高，避免触发服务器限制
3. 某些服务器不支持 Range 请求，会自动降级为单线程
4. 下载临时文件为 `.download` 后缀，完成后自动重命名
5. 断点续传的元数据文件为 `.meta` 后缀

## 示例代码

完整示例请查看：

- `example/downloader/media_downloader.py` - 基础用法示例
- `example/spider/media_spider.py` - 集成到爬虫示例

## 架构设计

```
MediaDownloader
├── 使用现有下载器 (默认cffi，也可用httpx/requests等)
├── Stream 模式 - 通过 $options 参数控制流式下载
├── DownloadTask - 任务配置
├── DownloadMeta - 断点续传元数据
├── ChunkInfo - 分块信息
└── 下载策略
    ├── 单线程流式下载 (小文件/不支持Range) - 自动启用 stream
    └── 多线程分块下载 (大文件/支持Range) - 并发请求
```

### Stream 模式

所有下载器都支持通过 `$options` 参数控制流式下载：

```python
from bricks import Request
from bricks.downloader.cffi import Downloader

downloader = Downloader()

# 方式1: 直接在 Request 中设置
request = Request(
    url="https://example.com/file.zip",
    method="GET",
    $options={"stream": True, "chunk_size": 8192}
)

# 方式2: 通过 options 参数设置
request = Request(url="https://example.com/file.zip", method="GET")
request.options["$options"] = {"stream": True, "chunk_size": 8192}

# 获取响应
response = downloader.fetch(request)

# 方式1: 使用 iter_content() 进行流式读取
for chunk in response.iter_content(chunk_size=8192):
    # 处理每个数据块
    process(chunk)

# 方式2: 直接访问 content（会自动从迭代器读取完整内容）
content = response.content  # 返回完整的 bytes
```

支持 stream 模式的下载器：

- ✅ `cffi.Downloader` (默认)
- ✅ `requests_.Downloader`
- ✅ `httpx_.Downloader`

**注意**: MediaDownloader 会自动在单线程下载时启用 stream 模式

## 许可

与 bricks 框架使用相同的许可协议
