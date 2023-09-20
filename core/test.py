import datetime

if __name__ == '__main__':
    my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

    # 定义每次获取的元素数量
    batch_size = 13

    # 使用循环遍历列表并处理每个批次的元素
    for i in range(0, len(my_list), batch_size):
        batch = my_list[i:i + batch_size]
        # 在这里对每个批次的元素进行处理
        print("Processing batch:", batch)

