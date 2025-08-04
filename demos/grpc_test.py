import json
import time

import grpc
from loguru import logger

from bricks.client.gprc_.generated import service_pb2_grpc, service_pb2


class GrpcClient:
    """gRPC Spider 客户端"""

    def __init__(self, host='localhost', port=50051):
        self.host = host
        self.port = port
        self.channel = grpc.insecure_channel(f'{self.host}:{self.port}')
        self.stub = service_pb2_grpc.ServiceStub(self.channel)

    def disconnect(self):
        """断开连接"""
        if self.channel:
            self.channel.close()
            logger.info("Disconnected from gRPC server")

    def rpc(self, method: str, data: dict = None, timeout=30):
        """调用"""
        try:
            if data:
                data_json = json.dumps(data)
            else:
                data_json = ""

            # 创建请求
            request = service_pb2.Request(
                data=data_json,
                metadata={"client": "test_client", "timestamp": str(time.time())}
            )

            logger.info(f"request: {request}")

            func = getattr(self.stub, method)
            # 发送请求
            response = func(request, timeout=timeout)

            if response.success:
                result_data = json.loads(response.data) if response.data else {}
                logger.info(f"success: {result_data}")
                return {"success": True, "data": result_data}
            else:
                logger.error(f"Failed: {response.error}")
                return {"success": False, "error": response.error}

        except grpc.RpcError as e:
            logger.error(f"gRPC error: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.error(f"Client error: {e}")
            return {"success": False, "error": str(e)}


def interactive_test():
    """交互式测试"""
    client = GrpcClient()

    try:
        res = client.rpc("health")
        if not res['success']:
            logger.error("服务器不可用")
            return

        print("\n=== gRPC 交互式测试 ===")
        print("输入种子数据 (JSON 格式), 输入 'quit' 退出")
        print("示例: {'page': 1} 或 {'page': 2, '$config': 1}")

        while True:
            try:
                user_input = input("\n请输入种子数据: ").strip()

                if user_input.lower() in ['quit', 'exit', 'q']:
                    break

                if not user_input:
                    continue

                # 解析用户输入
                try:
                    seeds = eval(user_input)  # 注意：生产环境应使用 json.loads
                    if not isinstance(seeds, dict):
                        print("错误: 种子数据必须是字典格式")
                        continue
                except:
                    print("错误: 无效的 JSON 格式")
                    continue

                # 处理种子
                result = client.rpc("execute", seeds)
                print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"错误: {e}")

    finally:
        client.disconnect()
        print("\n测试结束")


if __name__ == "__main__":
    interactive_test()
