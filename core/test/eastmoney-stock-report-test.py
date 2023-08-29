import sys

sys.path.append("..")
from service.EastmoneyStockReport import eastmoney

if __name__ == "__main__":
    eastmoney("839946", "华阳变速", None, None)