# ===================个股数据同步==============================
import sys

from service.DataSynAifin_bak import data_sys_aifin

if __name__ == '__main__':
    # 同步数据
    status = 2 if len(sys.argv) < 2 else int(sys.argv[1])
    data_sys_aifin("aifin_stock", "aifin_stock", True, status)
