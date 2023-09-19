# ===================宏观数据同步==============================
import sys

from service.DataSynAifin import data_sys_aifin

if __name__ == '__main__':
    status = 2 if len(sys.argv) < 2 else int(sys.argv[1])
    # 同步数据
    data_sys_aifin("aifin_macro", "aifin_macro", False, status)
