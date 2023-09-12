import sys

from service.MacroKuaixun import kuaixun_macro

if __name__ == '__main__':
    beginTime = None
    endTime = None
    if len(sys.argv) > 1:
        beginTime = sys.argv[1]  # 开始时间
        endTime = sys.argv[2]  # 结束时间"2023-08-28"
    kuaixun_macro(beginTime, endTime, True)
