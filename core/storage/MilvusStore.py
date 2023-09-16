###################### 存储类 ###############################################
# -*- coding: UTF-8 -*-
import copy

import torch
from langchain.docstore.document import Document
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Milvus
from langchain.text_splitter import RecursiveCharacterTextSplitter

embedding_model_dict = {
    "ernie-tiny": "nghuyong/ernie-3.0-nano-zh",
    "ernie-base": "nghuyong/ernie-3.0-base-zh",
    "text2vec-base": "shibing624/text2vec-base-chinese",
    "text2vec": "/root/model/text2vec-large-chinese",
    #"m3e-small": "/data/huggingface/moka-ai/m3e-base",
    #"m3e-small": "/root/.crawlab/huggingface/moka-ai/m3e-base",
    "m3e-base": "/root/data/huggingface/moka-ai/m3e-base",
    "m3e-local": "/Users/huangying/data/huggingface/moka-ai/m3e-base",
}
EMBEDDING_MODEL = "m3e-base"
EMBEDDING_DEVICE = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"


def load_and_split(docs: list[Document]) -> list[Document]:
    """Load documents and split into chunks."""
    _text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=20)
    related_docs = _text_splitter.split_documents(docs)
    return [doc for doc in related_docs if len(doc.page_content.strip()) > 20]


def storeData(docs: list[dict],collection_name:str,path:str="47.97.218.138:31865"):

    docList:list[Document] = []
    for doc in copy.deepcopy(docs):
        text = '' if 'text' not in doc else doc.pop("text")
        docx = Document(page_content=text,
                       metadata=doc)
        docList.append(docx)

    docs = load_and_split(docList)
    print("开始HuggingFaceEmbeddings切分")
    embeddings = HuggingFaceEmbeddings(model_name=embedding_model_dict[EMBEDDING_MODEL],
                                       model_kwargs={'device': EMBEDDING_DEVICE})
    print("进入存储阶段")
    count = 0
    obj = None
    while True and count < 3:
        try:
            obj = Milvus.from_documents(
                docs,
                embeddings,
                connection_args={"host": path.split(":")[0], "port": path.split(":")[1]},
                collection_name=collection_name,
            )
            break
        except Exception as e:
            print(f"error,写入矢量库异常,{e}")
            count += 1
    if not obj:
        raise Exception("写入矢量库异常")
    print(f"写入矢量库【{collection_name }】over")


if __name__ == '__main__':

    metadata = [{"source": "Web",
                "uniqueId": 'dd',
                "code": 'code',
                "name": 'stockName',
                "url": 'url',
                "date": 'dfv',
                "type": "eastmoney-stock-report",
                "createTime": 'dfff',
                "abstract": 'abstract',
                "title": 'df',
                "mediaName": 'erer',
                "text": 'texsdfsdfsfsdfsfsfsdfsfsttexsdfsdfsfsdfsfsfsdfsfsttexsdfsdfsfsdfsfsfsdfsfsttexsdfsdfsfsdfsfsfsdfsfsttexsdfsdfsfsdfsfsfsdfsfst'}]
    dd=[{"source": "Web", "uniqueId": "20230904082200648218300", "code": "002377", "name": "国创高新", "url": "http://caifuhao.eastmoney.com/news/20230904082200648218300", "date": "2023-09-04 08:22:01", "type": "eastmoney-stock-carticle", "createTime": "2023-09-05 00:30:31", "abstract": "中介：当天二手房预约带看量增50%。（我爱我家、三六五网、国创高新、三湘印象、世联行等）5、电解铝：终端应用高增，1-8月国内电解铝表观需求达3.3%", "title": "9.4日｜发财哥早盘", "mediaName": "发财哥_1688", "text": "\n今日计划观察。科创50，北证50都可以作为情绪指数。方向地产地产链实打实的政策利好，华为MATE60热销，带动华为产业链，5G滤波器。昨日复盘1、大消费：国务院日前印发《关于提高个人所得税有关专项附加扣除标准的通知》，决定提高3岁以下婴幼儿照护、子女教育、赡养老人个人所得税专项附加扣除标准/8月31日央行、国家金融监督管理总局发布《关于调整优化差别化住房信贷政策的通知》。（中央商场、西部牧业、国芳集团、国光连锁、麦趣尔等）2、杭州亚运会：第19届亚运会将于9月23日在浙江杭州开幕。据杭州文旅大数据预测，亚运会期间，杭州将迎来近年来最为密集的游客潮，外地游客量将超过2000万人次。（华媒控股、万事利、浙数文化、电魂网络、杭州解百等）3、新疆板块：外交部31日表示，中方将于今年10月在京举办第三届一带一路国际合作高峰论坛/随着“一带一路”建设持续推进，中国西部慢慢从“开放的末梢”变为“开放的前沿”。（新疆交建、新赛股份、西部牧业、麦趣尔、宝地矿业等）4、地产中介：上海“认房不认贷”首日，中介：当天二手房预约带看量增50%。（我爱我家、三六五网、国创高新、三湘印象、世联行等）5、电解铝：终端应用高增，1-8月国内电解铝表观需求达3.3%，超过市场之前2%的一致预期。（中国铝业、神火股份、云铝股份、南山铝业、新疆众和等）6、新能源汽车： 9月1日，工业和信息化部等七部门印发《汽车行业稳增长工作方案（2023—2024年）》，支持扩大新能源汽车消费。（比亚迪、长城汽车、长安汽车、安凯客车、中通客车等）宏观政策1、证监会：指导证券交易所制定发布了《关于股票程序化交易报告工作有关事项的通知》《关于加强程序化交易管理有关事项的通知》。2、北京：继广州、深圳后，9月1日上海、北京官宣执行“认房不认贷”政策。至此，一线城市都已宣布执行“认房不认贷”政策措施。3、证监会：制定并发布《关于高质量建设北京证券交易所的意见》。4、工信部：七部门印发汽车行业稳增长工作方案（2023—2024年），力争2023全年实现汽车销量2700万辆左右，同比增长约3%，其中新能源汽车销量900万辆左右，同比增长约30%。热点前瞻1、规避量化丨量化收割，寸草不生。建议避开有融券的中小盘公司。关注优质低价大盘白马股。世纪鼎利、西部牧业、杭州园林、梦天家居2、北交所｜9月1日晚，证监会发布了被称为“北交所深改19条”的《关于高质量建设北京证券交易所的意见》。多家券商已上线“一键开通北交所权限” ，并通宵测试验证上线。易实精密、三祥科技、华原股份、鼎智科技3、房地产及中介｜上海、北京也官宣执行“认房不认贷”政策。至此，一线城市都已宣布执行“认房不认贷”政策措施。上海“认房不认贷”执行首日：有楼盘访客量增近4倍；北京多楼盘连夜涨价，电话被打爆。我爱我家、三六五网、国创高新、世联行4、华为产业链｜华为Mate 60系列线下今日（9月3日）开售，较计划提前一周时间。诚迈科技、美芯晟、同兴达、唯捷创芯5、煤炭｜海外价格大涨且进口煤已高于国内。海外三大动力煤指数从7月底以来持续上涨。煤炭板块缺乏机构持仓，占比很低。因为商品市场表现强势，未来煤炭板块还会有行情。中国神华、晋控煤业、兖矿能源、山西焦煤6、钼｜【建投金属】8月钼需求再超预期！招标量有望超12000吨！需求强势，一定重视产业升级机会。经济虽有压力，但国内钼近几年需求复合增速约15%。金钼股份、洛阳钼业、中金黄金、中国中铁7、禽｜天风：估值安全边际凸显，重视周期反转机会！白鸡产业磨底近3年，产能持续去化。祖代引种缺口已成，且种鸡结构中部分换羽难度较大，效率不高，缺口或更大。重视白鸡空间！圣农发展、益生股份、民和股份、禾丰股份追加内容\n本文作者可以追加内容哦 !"}]
    storeData(dd, f"aifin_stock_{1212}", "36.138.93.247:31395")