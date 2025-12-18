# -*- coding: utf-8 -*-
"""
Redis 数据导出为 CSV 示例
"""
import json
from pathlib import Path

from bricks.db.redis_ import Redis


def example_basic_export():
    """基本导出示例"""
    redis = Redis()

    # 准备测试数据 - set 类型
    redis.delete("test:users:set")
    for i in range(100):
        user = {
            "id": i,
            "name": f"User_{i}",
            "age": 20 + i % 50,
            "email": f"user{i}@example.com",
        }
        redis.sadd("test:users:set", json.dumps(user))

    # 导出为 CSV
    result = redis.export_to_csv(
        "test:users:set",
        "output/users_set.csv",
        chunk_size=20,
    )
    print(f"Set 导出结果: {result}")


def example_zset_with_score():
    """导出 zset 并包含 score"""
    redis = Redis()

    # 准备测试数据 - zset 类型
    redis.delete("test:scores")
    for i in range(50):
        student = {
            "student_id": f"S{i:04d}",
            "name": f"Student_{i}",
            "class": f"Class_{i % 5}",
        }
        # score 作为成绩
        redis.zadd("test:scores", {json.dumps(student): 60 + i % 40})

    # 导出，包含 score 列
    result = redis.export_to_csv(
        "test:scores",
        "output/scores_with_score.csv",
        include_score=True,
        headers=["student_id", "name", "class", "score"],  # 自定义表头顺序
    )
    print(f"Zset 导出结果: {result}")


def example_list_export():
    """导出 list 类型"""
    redis = Redis()

    # 准备测试数据 - list 类型
    redis.delete("test:logs")
    for i in range(200):
        log = {
            "timestamp": f"2024-01-{(i % 30) + 1:02d} 10:00:00",
            "level": ["INFO", "WARNING", "ERROR"][i % 3],
            "message": f"Log message {i}",
            "user_id": i % 10,
        }
        redis.rpush("test:logs", json.dumps(log))

    # 导出为 CSV，带进度回调
    def progress(current, total):
        percentage = (current / total * 100) if total > 0 else 0
        print(f"进度: {current}/{total} ({percentage:.1f}%)")

    result = redis.export_to_csv(
        "test:logs",
        "output/logs.csv",
        chunk_size=50,
        progress_callback=progress,
    )
    print(f"List 导出结果: {result}")


def example_hash_export():
    """导出 hash 类型"""
    redis = Redis()

    # 准备测试数据 - hash 类型
    redis.delete("test:config")
    configs = {
        "database.host": json.dumps({"value": "localhost", "type": "string"}),
        "database.port": json.dumps({"value": 3306, "type": "int"}),
        "cache.ttl": json.dumps({"value": 3600, "type": "int"}),
        "cache.enabled": json.dumps({"value": True, "type": "bool"}),
    }
    redis.hset("test:config", mapping=configs)

    # 导出
    result = redis.export_to_csv(
        "test:config",
        "output/config.csv",
    )
    print(f"Hash 导出结果: {result}")


def example_tsv_export():
    """导出为 TSV 格式"""
    redis = Redis()

    # 准备测试数据
    redis.delete("test:products")
    for i in range(30):
        product = {
            "sku": f"PRD-{i:05d}",
            "name": f"Product {i}",
            "price": round(10 + i * 1.5, 2),
            "stock": i * 10,
            "category": ["Electronics", "Clothing", "Food"][i % 3],
        }
        redis.sadd("test:products", json.dumps(product))

    # 导出为 TSV
    result = redis.export_to_csv(
        "test:products",
        "output/products.tsv",
        delimiter="\t",  # 使用制表符作为分隔符
    )
    print(f"TSV 导出结果: {result}")


def example_nested_data():
    """展平嵌套的 JSON 数据"""
    redis = Redis()

    # 准备嵌套数据
    redis.delete("test:nested")
    for i in range(20):
        data = {
            "id": i,
            "user": {
                "name": f"User_{i}",
                "contact": {
                    "email": f"user{i}@example.com",
                    "phone": f"123-456-{i:04d}",
                }
            },
            "orders": [
                {"order_id": f"O{i}-1", "amount": 100 + i},
                {"order_id": f"O{i}-2", "amount": 200 + i},
            ]
        }
        redis.sadd("test:nested", json.dumps(data))

    # 导出，展平嵌套结构
    result = redis.export_to_csv(
        "test:nested",
        "output/nested_flattened.csv",
        flatten=True,
        max_depth=2,  # 展平深度为 2
    )
    print(f"嵌套数据导出结果: {result}")

    # 导出，不展平
    result = redis.export_to_csv(
        "test:nested",
        "output/nested_raw.csv",
        flatten=False,
    )
    print(f"嵌套数据(不展平)导出结果: {result}")


def example_append_mode():
    """追加模式导出"""
    redis = Redis()

    # 第一次导出
    redis.delete("test:batch1")
    for i in range(10):
        redis.sadd("test:batch1", json.dumps({"id": i, "batch": 1}))

    result1 = redis.export_to_csv(
        "test:batch1",
        "output/batches.csv",
        write_mode="w",  # 覆盖模式
    )
    print(f"第一批导出: {result1}")

    # 第二次导出（追加）
    redis.delete("test:batch2")
    for i in range(10, 20):
        redis.sadd("test:batch2", json.dumps({"id": i, "batch": 2}))

    result2 = redis.export_to_csv(
        "test:batch2",
        "output/batches.csv",
        write_mode="a",  # 追加模式
        headers=["id", "batch"],  # 追加时需要指定表头
    )
    print(f"第二批导出（追加）: {result2}")


def example_custom_headers():
    """自定义表头和字段"""
    redis = Redis()

    # 准备数据
    redis.delete("test:custom")
    for i in range(20):
        data = {
            "user_id": i,
            "username": f"user_{i}",
            "email": f"user{i}@example.com",
            "age": 20 + i,
            "extra_field": "not_in_headers",  # 这个字段不会被导出
        }
        redis.sadd("test:custom", json.dumps(data))

    # 只导出指定的列
    result = redis.export_to_csv(
        "test:custom",
        "output/custom_headers.csv",
        headers=["user_id", "username", "email"],  # 只导出这些列
        auto_detect_headers=False,
    )
    print(f"自定义表头导出: {result}")


def example_error_handling():
    """错误处理示例"""
    redis = Redis()

    # 准备混合数据（包含一些无效 JSON）
    redis.delete("test:mixed")
    redis.sadd("test:mixed", json.dumps({"id": 1, "name": "Valid 1"}))
    redis.sadd("test:mixed", "invalid json string")  # 无效 JSON
    redis.sadd("test:mixed", json.dumps({"id": 2, "name": "Valid 2"}))
    redis.sadd("test:mixed", "{broken json")  # 无效 JSON
    redis.sadd("test:mixed", json.dumps({"id": 3}))  # 缺少字段

    # 跳过错误继续导出
    result = redis.export_to_csv(
        "test:mixed",
        "output/mixed_skip_errors.csv",
        skip_errors=True,
        default_value="N/A",  # 缺失字段的默认值
    )
    print(f"跳过错误导出: {result}")


def main():
    """运行所有示例"""
    # 创建输出目录
    Path("output").mkdir(exist_ok=True)

    print("=" * 60)
    print("1. 基本导出 (Set)")
    print("=" * 60)
    example_basic_export()

    print("\n" + "=" * 60)
    print("2. Zset 导出（包含 Score）")
    print("=" * 60)
    example_zset_with_score()

    print("\n" + "=" * 60)
    print("3. List 导出（带进度）")
    print("=" * 60)
    example_list_export()

    print("\n" + "=" * 60)
    print("4. Hash 导出")
    print("=" * 60)
    example_hash_export()

    print("\n" + "=" * 60)
    print("5. TSV 格式导出")
    print("=" * 60)
    example_tsv_export()

    print("\n" + "=" * 60)
    print("6. 嵌套数据展平")
    print("=" * 60)
    example_nested_data()

    print("\n" + "=" * 60)
    print("7. 追加模式导出")
    print("=" * 60)
    example_append_mode()

    print("\n" + "=" * 60)
    print("8. 自定义表头")
    print("=" * 60)
    example_custom_headers()

    print("\n" + "=" * 60)
    print("9. 错误处理")
    print("=" * 60)
    example_error_handling()

    print("\n" + "=" * 60)
    print("所有示例运行完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
