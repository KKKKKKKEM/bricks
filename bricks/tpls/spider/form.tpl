from bricks import logger
from bricks.plugins import scripts
from bricks.spider import form


class {SPIDER}(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            # 修改为你自己的初始化逻辑
            init=[
                form.Init(func=lambda: {{"page": 1}})
            ],
            spider=[

                # 定义下载节点, 指定下载规则
                form.Download(
                    # 请求的 URL
                    url={URL!r},
                    # 请求方法
                    method={METHOD!r},
                    # 请求的 URL 参数
                    params={PARAMS!r},
                    # 请求的 body, 全部设置为字典时需要和 headers 配合, 规则如下
                    # 如果是 json 格式, headers 里面设置 Content-Type 为 application/json
                    # 如果是 form urlencoded 格式, headers 里面设置 Content-Type 为 application/x-www-form-urlencoded
                    # 如果是 form data 格式, headers 里面设置 Content-Type 为 multipart/form-data
                    body={BODY!r},
                    # 请求标头 Union[Header, dict]
                    headers={HEADERS!r},
                    # 请求 cookies, 一般不设置, 类型为 Dict[str, str]
                    cookies={COOKIES!r},
                    # 请求额外选项
                    options={OPTIONS!r},
                    # 请求超时时间, 填入 ... 为默认
                    timeout=...,
                    # 是否允许重定向
                    allow_redirects={ALLOW_REDIRECTS!r},
                    # 请求代理 Optional[str], 如 http://127.0.0.1:7890
                    proxies={PROXIES!r},
                    # 代理 Key
                    proxy={PROXY!r},
                    # 判断成功动态脚本, 字符串形式, 如通过 403 状态码可以写为: 200 <= response.status_code < 400 or response.status_code == 403
                    ok=...,
                    # 最大重试次数, 默认为 5
                    max_retry={MAX_RETRY!r},
                    # 是否使用 session 进行请求, 默认为 False
                    use_session={USE_SESSION!r},

                ),

                # 请求后判断相应是否成功, 并且输出相应内容
                form.Task(
                    func=scripts.is_success,
                    kwargs={{
                        "match": [
                            "context.response.ok"
                        ],
                        "post": [
                            "logger.debug(f'请求成功, 响应为: {{context.response.text}}')"
                        ]
                    }}
                ),

                # 编写解析规则, 可以指定自定义函数, 也可以指定内置引擎
                # 返回值为 字典 / 列表
                form.Parse(
                    func=lambda context: {{
                        "status_code": context.response.status_code,
                        "size": context.response.size,
                        "seeds": dict(context.seeds),
                        "url": context.response.url,
                        "method": context.request.method
                    }}
                ),

                # 存储解析数据, 可以指定自定义函数, 也可以指定内置引擎
                form.Pipeline(
                    func=lambda context: logger.debug(f'解析结果: {{context.items}}'),
                    # 确认种子已经完成爬取 -> 从任务队列中删除该种子
                    success=True
                )
            ],
            # 编写自定义事件
            events={{}}
        )


if __name__ == '__main__':
    spider = {SPIDER}()
    spider.run()
