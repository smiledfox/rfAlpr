from openalpr import Alpr
import multiprocessing
from multiprocessing import Process, Queue, Lock, cpu_count
import threading
import os
import time, datetime
import Image
import pyexiv2
import cv2
import oss2
import json

# Internal variable #
#
postprocess_min_confidence = 75

#
RAM_ROOT_PATH_PREFIX = '/ramdisk/'
ROM_ROOT_PATH_PREFIX = '../'
ALPR_IMAGES_TEMP_FILE = RAM_ROOT_PATH_PREFIX + 'temp/'
ALPR_RESULT_BUFFER = RAM_ROOT_PATH_PREFIX + 'result/'
ALPR_DEBUG_FLODER = '/nas/'

def get_mac_address():
    import uuid
    mac=uuid.UUID(int = uuid.getnode()).hex[-12:].lower()
    return '%s%s%s%s%s%s' % (mac[0:2],mac[2:4],mac[4:6],mac[6:8],mac[8:10],mac[10:])
##    return ":".join([mac[e:e+2] for e in range(0,11,2)])

aliyun_images_tick = 'upload/photos/ocr/ing/' +  get_mac_address()
aliyun_images = 'upload/photos/ocr/ing/' +  get_mac_address() + '/'
aliyun_images_result = 'upload/photos/ocr/result/' + get_mac_address() + '/'

print "images = " + aliyun_images + " result = " + aliyun_images_result

################################
class myAlprProcessing (multiprocessing.Process):
  def __init__(self, alprWorkQueue, alprQueueLock, aliyunWorkQueue, aliyunQueueLock, name, debug):
    multiprocessing.Process.__init__(self)
    self.name = name
    self.alprWorkQueue = alprWorkQueue
    self.alprQueueLock = alprQueueLock
    self.aliyunWorkQueue = aliyunWorkQueue
    self.aliyunQueueLock = aliyunQueueLock
    self.bucket = oss2.Bucket(oss2.Auth(os.getenv('OSS2_ACCESS_KEY_ID'), os.getenv('OSS2_ACCESS_KEY_SECRET')), 'http://oss-cn-qingdao.aliyuncs.com', 'runffphoto')
    self.debug = debug

  def run(self):
    print("myAlprProcessing Start")
    self.process_data(self.name, self.alprWorkQueue, self.alprQueueLock, self.aliyunWorkQueue, self.aliyunQueueLock)

  def process_data(self, threadName, alprWorkQueue, alprQueueLock, aliyunWorkQueue, aliyunQueueLock):
    while True:
      alprQueueLock.acquire()
      if not alprWorkQueue.empty():
        key = alprWorkQueue.get()
        alprQueueLock.release()
        if key == "exit":
          print "%s exit" % (threadName)
          break
        else:
          print "%s processing %s" % (threadName, key)
        filePath = self.run_downloadImage(key)
        if filePath != '':
          # Debug file
          if self.debug >= 2:
            os.system("cp -f " + filePath + " " + ALPR_DEBUG_FLODER)
          # Image rotate
          self.run_imageRotate(filePath)
          # alpr main
          alpr = Alpr("us", "/usr/share/openalpr/config/openalpr.defaults.conf", "/usr/share/openalpr/runtime_data")
          if not alpr.is_loaded():
            print("Error loading OpenALPR")
            alpr.unload()
            continue
          alpr.set_top_n(20)
          alpr.set_default_region("md")
          recognizeResults = self.run_alprRecognize(alpr, filePath)
          # Call when completely done to release memory
          alpr.unload()
          # Post process
          self.run_postProcess(key, filePath, recognizeResults)
            # En aliyun upload queue
          aliyunQueueLock.acquire()
          aliyunWorkQueue.put(key)
          aliyunQueueLock.release()
          # Delete temp image file
          os.remove(filePath)
      else:
        alprQueueLock.release()
        time.sleep(1)

  def run_downloadImage(self, key):
    filePath = ''
    try:
      filePath = ALPR_IMAGES_TEMP_FILE + os.path.basename(key)
      self.bucket.get_object_to_file(key, filePath)
    except:
      print 'download failed, key=' + key
    return filePath

  def run_imageRotate(self, filePath):
    try:
      # Clip image when wide less than high
      metadata = pyexiv2.metadata.ImageMetadata(filePath)
      metadata.read()
      # 0   - 0
      # +90 - 6
      # -90 - 8
      # 180 - 3
      try:
        orientation = metadata["Exif.Image.Orientation"].value
        if orientation == 8:
          image = Image.open(filePath)
          image = image.rotate(90)
          os.remove(filePath)
          image.save(filePath, 'JPEG')
        elif orientation == 6:
          image = Image.open(filePath)
          image = image.rotate(-90)
          os.remove(filePath)
          image.save(filePath, 'JPEG')
      except:
        print 'run_imageRotate, bad meta data, filePath=' + filePath
    except:
      print 'run_imageRotate failed, filePath=' + filePath

  def run_alprRecognize(self, alpr, filePath):
    try:
      # Run the recognization
      results = alpr.recognize_file(filePath)
      print("processing %s time_ms %sms" % (os.path.basename(filePath), results['processing_time_ms']))
      return results['results']
    except:
      print 'run_alprRecognize failed, filePath=' + filePath
      return ''

#key upload/photos/ocr/ing/1169/b827eb047cb3_dsc_4755_1472680_08c1afcbdb19a38a7064570d53570768.jpg
  def run_postProcess(self, key, filePath, recognizeResults):
    plateValidation = False
    try:
      jsonString = json.dumps(recognizeResults)
      resultKey = key.replace(aliyun_images, aliyun_images_result, 1)
      resultKey = resultKey.replace('.jpg', '.txt', 1)
      resultKeyFile = ALPR_RESULT_BUFFER + os.path.basename(resultKey)
      # Copy file for debug purpose
      if self.debug == 1:
        if len(jsonString) > 2:
          debugKeyFile = os.path.join(ALPR_DEBUG_FLODER, str(datetime.date.today())[0:10])
          if not os.path.exists(debugKeyFile):
            os.mkdir(debugKeyFile)
          debugKeyFile = os.path.join(debugKeyFile, os.path.basename(resultKey))
          debugKeyFile = debugKeyFile.replace('.txt', '.log', 1)
          file_object = open(debugKeyFile, 'w')
          try:
            file_object.write(jsonString)
          finally:
            file_object.close()
      # Result Sanity Check and find the result
      for plate in recognizeResults:
        for candidates in plate['candidates']:
          if candidates['matches_template'] == 1:
            plateValidation = True
            break
      if plateValidation == True:
        # Save to file
        # Write result to file
        file_object = open(resultKeyFile, 'w')
        try:
          file_object.write(jsonString)
        finally:
          file_object.close()
    except:
      print 'run_postProcess failed, key=' + key

################################
class myAliyunProcessing(threading.Thread):
  def __init__(self, aliyunWorkQueue, aliyunQueueLock, name, debug):
    threading.Thread.__init__(self)
    self.name = name
    self.aliyunWorkQueue = aliyunWorkQueue
    self.aliyunQueueLock = aliyunQueueLock
    self.thread_stop = False
    self.debug = debug
    self.bucket = oss2.Bucket(oss2.Auth(os.getenv('OSS2_ACCESS_KEY_ID'), os.getenv('OSS2_ACCESS_KEY_SECRET')), 'http://oss-cn-qingdao.aliyuncs.com', 'runffphoto')

  def run(self):
    print("myAliyunProcessing Start")
    while True:
      self.aliyunQueueLock.acquire()
      if not self.aliyunWorkQueue.empty():
        key = self.aliyunWorkQueue.get()
        self.aliyunQueueLock.release()
        # Upload result
        self.run_uploadResult(key)
      else:
        self.aliyunQueueLock.release()
        time.sleep(1)

  def run_uploadResult(self, key):
    try:
      resultKey = key.replace(aliyun_images, aliyun_images_result, 1)
      resultKey = resultKey.replace('.jpg', '.txt', 1)
      resultKeyFile = ALPR_RESULT_BUFFER + os.path.basename(resultKey)
      if os.path.exists(resultKeyFile):
        self.bucket.put_object_from_file(resultKey, resultKeyFile)
        print "Delete local file " + resultKeyFile
        os.remove(resultKeyFile)
      # Destory File
      print "Delete Aliyun " + key
      self.bucket.delete_object(key)
    except:
      print 'run_uploadResult failed, key=' + key

################################
## Main Functions ##
def clearBuffer():
  if not os.path.exists(ALPR_IMAGES_TEMP_FILE):
    os.mkdir(ALPR_IMAGES_TEMP_FILE)
    os.chmod(ALPR_IMAGES_TEMP_FILE, stat.S_IRWXO|stat.S_IRWXG|stat.S_IRWXU)
  if not os.path.exists(ALPR_RESULT_BUFFER):
    os.mkdir(ALPR_RESULT_BUFFER)
    os.chmod(ALPR_RESULT_BUFFER, stat.S_IRWXO|stat.S_IRWXG|stat.S_IRWXU)
  ## Ramdisk create by /etc/rc.local
  os.system("rm -rf " + ALPR_IMAGES_TEMP_FILE + "*")
  ## Result file
  os.system("rm -rf " + ALPR_RESULT_BUFFER + "*")

def run_fileLoad(workQueue, queueLock):
  clearBuffer()
  bucket = oss2.Bucket(oss2.Auth(os.getenv('OSS2_ACCESS_KEY_ID'), os.getenv('OSS2_ACCESS_KEY_SECRET')), 'http://oss-cn-qingdao.aliyuncs.com', 'runffphoto')
  # Retrive project folder list
  proj_folder_list = oss2.ObjectIterator(bucket, prefix=aliyun_images, delimiter='/')
  for projInfo in proj_folder_list:
    print projInfo.key
    if projInfo.key != aliyun_images:
      configPath = projInfo.key+'config.txt'
      localConfigPath = '/usr/share/openalpr/runtime_data/postprocess/us.patterns'
      if bucket.object_exists(configPath):
        # Fill in the Queue
        queueLock.acquire()
        object_list = oss2.ObjectIterator(bucket, prefix=projInfo.key)
        object_list_length = 0
        for objectInfo in object_list:
          if os.path.splitext(objectInfo.key)[1] == '.jpg':
            workQueue.put(objectInfo.key)
            object_list_length += 1
        if object_list_length > 0:
          #copy config.txt file to aplr config
          bucket.get_object_to_file(configPath, localConfigPath)
        queueLock.release()
        # Wait for queue to empty
        if object_list_length != 0:
          while not workQueue.empty():
            time.sleep(5)
            pass
  print "run_fileLoad Finished"

def main_func():
  ## Start subTask
  alprThreadNum = cpu_count()*3
  alprThreads = []
  alprWorkQueue = Queue()
  alprQueueLock = Lock()
  aliyunThreads = []
  aliyunWorkQueue = Queue()
  aliyunQueueLock = Lock()

  # Create new threads for recognization
  for tNum in range(1, alprThreadNum+1):
     thread = myAlprProcessing(alprWorkQueue, alprQueueLock, aliyunWorkQueue, aliyunQueueLock, ('T'+str(tNum)), 1)
     thread.start()
     alprThreads.append(thread)

  for tNum in range(1, aliyunThreadNum+1):
    thread = myAliyunProcessing(aliyunWorkQueue, aliyunQueueLock, ('T'+str(tNum)), 0)
    thread.setDaemon(True)
    thread.start()
    aliyunThreads.append(thread)

  ## Load from file
  while True:
    try:
      run_fileLoad(alprWorkQueue, alprQueueLock)
      time.sleep(10)
    except Exception, exc:
      print exc

  # Notify threads it's time to exit
  for t in alprThreads:
    workQueue.put("exit")

  # Wait for all threads to complete
  for t in alprThreads:
    t.join()
  print "Exiting Main Thread"
###
if __name__== '__main__':
    main_func()
