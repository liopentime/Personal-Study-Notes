import asyncio

# asyncio 库核心关键词：async await

# 核心概念：协程 一种可以在执行耗时任务中暂停,并在耗时任务完成后恢复的对象

# async 关键字可以在协程函数定义 和 异步上下文管理中使用
async def g():

    result = await f()

    return result
# 协程函数g()被调用时正常执行,直到遇到await表达式,这时候协程函数g()会暂停执行,把控制权交给事件循环,直到另一个异步任务f()的结果返回
# f()必须是一个可等待的对象比如另一个协程函数或者定义了__await__()特殊方法并返回迭代器的对象
import time
#
# def count():
#     print("1")
#     time.sleep(1)
#     print("2")
#     time.sleep(1)
#
# def main():
#
#     for _ in range(2):
#         count()
#
#
# if __name__ == "__main__":
#
#     start = time.perf_counter()
#     main()
#     end = time.perf_counter()
#     elapsed = end - start
#     print(f"{__file__} executed in {elapsed} seconds")

# async def count_async():
#     print("1")
#     await asyncio.sleep(1)
#     print("2")
#     await asyncio.sleep(1)
#
# async def main_async():
#
#     await asyncio.gather(count_async(), count_async())
#
#
# if __name__ == "__main__":
#
#     start = time.perf_counter()
#     asyncio.run(main_async())
#     end = time.perf_counter()
#     elapsed = end - start
#     print(f"{__file__} executed in {elapsed} seconds")

# 常见的异步I/O编程模式
# 协程连接：协程函数是可等待对象，await + 可等待对象
import random

# 通过user_id从数据库中获取user数据
# async def fetch_user(user_id:int) -> dict:
#     delay = random.uniform(0.5,1.5)
#     print(f"正在从MySQL获取{user_id}的数据")
#     await asyncio.sleep(delay)
#     user = {"id":user_id,"name":f"User{user_id}"}
#     return user
#
# async def fetch_posts(user:dict)->None:
#     delay = random.uniform(0.5,1.5)
#     print(f"正在获取用户{user["name"]}发布的帖子")
#     await asyncio.sleep(delay)
#     posts = [f"Posts {_} by {user["name"]}" for _ in range(3)]
#     print(f"{user['name']}发送了{len(posts)}条帖子，FETCH finished in {delay} seconds")
#     for ps in posts:
#         print(ps)
#
# async def get_user_with_posts(user_id):
#     user = await fetch_user(user_id)
#     await fetch_posts(user)
#
# async def main():
#     user_ids = [1, 2, 3]
#     start = time.perf_counter()
#     await asyncio.gather(
#         *(get_user_with_posts(user_id) for user_id in user_ids)
#     )
#     end = time.perf_counter()
#     print(f"\n==> Total time: {end - start:.2f} seconds")
#
# if __name__ == "__main__":
#     random.seed(444)
#     asyncio.run(main())

# 基于生产者消费者架构的异步队列
# queue = asyncio.Queue()

# 我们使用队列的方案完成上个案例的改造
from typing import List
async def producer(queue:asyncio.Queue,user_ids:List[int]):

    async def fetch_user(user_id):
        delay = random.uniform(0.5, 2.0)
        print(f"Producer: fetching user by {user_id=}...")
        await asyncio.sleep(delay)
        user = {"id": user_id, "name": f"User{user_id}"}
        print(f"Producer: fetched user with {user_id=} (done in {delay:.1f}s)")
        await queue.put(user)

    await asyncio.gather(*(fetch_user(uid) for uid in user_ids))

    for _ in range(len(user_ids)):
        await queue.put(None)

async def consumer(queue:asyncio.Queue):
    while True:
        user = await queue.get()
        if user is None:
            break

        delay = random.uniform(0.5, 1.5)
        print(f"正在获取用户{user["name"]}发布的帖子")
        await asyncio.sleep(delay)
        posts = [f"Posts {_} by {user["name"]}" for _ in range(3)]
        print(f"{user['name']}发送了{len(posts)}条帖子，FETCH finished in {delay} seconds")
        for ps in posts:
            print(ps)

async def main():
    queue = asyncio.Queue()
    user_ids = [1, 2, 3]

    start = time.perf_counter()
    await asyncio.gather(
        producer(queue, user_ids),
        *(consumer(queue) for _ in user_ids),
    )
    end = time.perf_counter()
    print(f"\n==> Total time: {end - start:.2f} seconds")

if __name__ == "__main__":
    random.seed(444)
    asyncio.run(main())

