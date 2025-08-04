from bricks.utils import pandora

pandora.require("grpcio-tools==1.74.0")
import os
import subprocess

dir_name = os.path.dirname(os.path.dirname(__file__))


def generate_grpc_code():
    """生成 gRPC Python 代码"""
    proto_dir = os.path.join(dir_name, "protos")
    output_dir = os.path.join(dir_name, "generated")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 生成 Python gRPC 代码
    cmd = [
        "python", "-m", "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={output_dir}",
        f"--pyi_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        f"{proto_dir}/service.proto"
    ]

    subprocess.run(cmd, check=True)
    print(f"gRPC code generated in {output_dir}/")


if __name__ == "__main__":
    generate_grpc_code()
