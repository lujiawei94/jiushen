from functools import wraps

REEPOSITORY_Dic = {}
def Singleton(cls):
    @wraps(cls)
    def getinstance(*args, **kwargs):
        if cls not in REEPOSITORY_Dic:
            REEPOSITORY_Dic[cls] = cls(*args, **kwargs)
        return REEPOSITORY_Dic[cls]
    return getinstance

def delete_singleton():
    global REEPOSITORY_Dic
    REEPOSITORY_Dic = {}

