from logging import basicConfig as _basicConfig, info as _info, error as _error, NOTSET as _NOTSET
from threading import Thread as _Thread
from time import sleep as _sleep
from urllib3 import disable_warnings as _disable_warnings
from urllib3.exceptions import InsecureRequestWarning as _InsecureRequestWarning
from multiprocessing import cpu_count as _cpu_count
from base64 import b64encode as _b64encode
from requests import post as _post
from xml.etree.ElementTree import fromstring as _fromstring
_disable_warnings(_InsecureRequestWarning)

WAIT_TIME = 1
MAX_RUN_COUNT = _cpu_count()

responseResult = []

class _SoapConsumeUpload:
    def __init__(self, targetURL:str, targetUserName:str, targetPassword:str, reportLocalPath:str) -> None:
        self.targetWsdlURL =  targetURL + "/xmlpserver/services/v2/CatalogService?wsdl"
        self.getCredentials  = f"<v2:userID>{targetUserName}</v2:userID> <v2:password>{targetPassword}</v2:password>"
        self.header = {"Content-Type": "text/xml;charset=UTF-8"}
        self.reportLocalPath = reportLocalPath

    def _callPostMethod(self, body:str, timeout=60, verify=False, **kargs): 
        _message = kargs.get('message') 
        _url = kargs.get('url', self.targetWsdlURL) 
        _header = kargs.get('header', self.header)
        _info(f'{_message} : {body}')
        response =  _post(_url, data=body.replace('##CREDENTIAL##', self.getCredentials), headers=_header, verify=verify, timeout=timeout)
        _info(f'{_message} : {response.status_code}')
        _info(response.text)
        return response

    def uploadObject(self, path:str):
        _info(f'Upload object processs started for {path}')
        responseMessage:str = '_error : File failed to uploaded : ' + path
        try:
            fileName, fileExtension = path.split('/')[-1].split('.')
            fileLocation = f'{self.reportLocalPath}/{fileName}.{fileExtension}'
            objectZippedData = _b64encode(open(fileLocation, 'rb').read()).decode('utf-8')
            self._deleteObject(path)
            body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2"><soapenv:Header/><soapenv:Body>
                <v2:uploadObject>
                    <v2:reportObjectAbsolutePathURL>{path}</v2:reportObjectAbsolutePathURL>
                    <v2:objectType>{fileExtension}z</v2:objectType>
                    <v2:objectZippedData>{objectZippedData}</v2:objectZippedData>
                    ##CREDENTIAL##
                </v2:uploadObject></soapenv:Body></soapenv:Envelope>"""
            response = self._callPostMethod(body, message = 'Upload Function Called')
            if response.status_code //100 == 2:
                responseMessage =  'Success : File uploaded successfully : ' + path
            else:
                responseContent = response.content.decode("utf-8")
                responseRoot = _fromstring(responseContent)
                faultString = responseRoot[0][0][1].text
                responseMessage = '_error : %s : %s'%(faultString.__str__().replace(':', ''), path)
        except Exception as e:
            _error(str(e))
            responseMessage:str = '_error : %s : %s'%(e.__str__().replace(':', ''), path)
        finally:
            _info(f'Upload processs completed for {path} -- {responseMessage}')
            return responseMessage

    
    def _deleteObject(self, path:str) -> None:
        _info(f'Delete object processs started for {path}')
        body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2"><soapenv:Header/><soapenv:Body><v2:deleteObject>
                        <v2:objectAbsolutePath>{path}</v2:objectAbsolutePath>
                        ##CREDENTIAL##
                    </v2:deleteObject></soapenv:Body></soapenv:Envelope>"""
        response = self._callPostMethod(body,message = 'Delete function Called')
        if response.status_code // 100 == 2: 
            self._objectExistsCheck(path)
        _info(f'Delete object processs completed with status as {response.status_code}')

    def _objectExistsCheck(self, path:str):
        _info(f'Object Exists Check processs started for {path}')
        body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2"><soapenv:Header/><soapenv:Body>
                    <v2:objectExist>
                        <v2:reportObjectAbsolutePath>?</v2:reportObjectAbsolutePath>
                        ##CREDENTIAL##
                    </v2:objectExist></soapenv:Body></soapenv:Envelope>"""
        while(True):
            response = self._callPostMethod(body,message = 'objectExistsCheck function Called')
            if response.text.__contains__('<objectExistReturn>false</objectExistReturn>'): break
            _sleep(WAIT_TIME)
        _info(f'Object Exists Check processs Completed for {path}')    
    
def multiThreadingUploadBI(SoapObj:_SoapConsumeUpload, reportRelativePath:str) -> None:
    _info(f'uploadBI processs started for {reportRelativePath}')
    responseString = SoapObj.uploadObject(reportRelativePath.strip())    
    responseResult.append(responseString)
    _info(f'uploadBI processs completed for {reportRelativePath}')

def uploadBI(url, user_name, password, reportRelativePath:str, reportLocalPath:str):
    try:
        splitPath = reportLocalPath.split('/')
        path = '/'.join(splitPath[:splitPath.index('IN')+1])
        requestID = splitPath[splitPath.index('IN')+1]
        logFilePath = path + f'/_error/LOG_{requestID}.txt'
        _basicConfig(filename=logFilePath, filemode='a+', format='%(asctime)s - %(levelname)s - %(message)s', level=_NOTSET)
    except Exception as e: pass  
    _info('uploadBI processs started')
    _info(f'MAX_RUN_COUNT: {MAX_RUN_COUNT}')
    _info(f'WAIT_TIME: {WAIT_TIME}')
    soapConsumeObject = _SoapConsumeUpload(targetURL= url,targetUserName= user_name,targetPassword= password, reportLocalPath = reportLocalPath)
    
    threadList = [_Thread(target=multiThreadingUploadBI, args=(soapConsumeObject, path), name=path) for path in reportRelativePath.split(',')]
    for i in range(0,len(threadList),MAX_RUN_COUNT):
        runThreadList  = threadList[i:i+MAX_RUN_COUNT]
        _info(runThreadList)
        [i.start() for i in runThreadList]
        [i.join() for i in runThreadList]
    _info(responseResult)
    _info('uploadBI processs finsished')
    return ';'.join(responseResult)

if __name__ == "__main__":
#     a= uploadBI('https://analyticsdigitalinstance-bmfbdl6iatvi-bo.analytics.ocp.oraclecloud.com/',
#                 'sushilkumar.jadhav85@gmail.com',
#                 'Internal@123',
#                 '/Dev/BI Reports/AP_TurnOver_Ratio_Report.xdo'
#                 'D:/DPLOY_POC/OUT/7077' )
