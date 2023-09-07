# coding=utf-8
import os
import os.path
import json
import csv
import time
import argparse
import logging
import datetime
import concurrent.futures
import pandas as pd


class PicInfo:
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.caption = str(data.get("caption", ""))
        self.url = str(data.get("url", ""))
        self.key = str(data.get("key", ""))
        #self.status = str(data.get("status", ""))
        #self.error_message = str(data.get("error_message", ""))
        #self.width = str(data.get("width", ""))
        #self.height = str(data.get("height", ""))
        self.original_width = str(data.get("original_width", ""))
        self.original_height = str(data.get("original_height", ""))
        #self.exif = str(data.get("exif", ""))
        #self.sha256 = str(data.get("sha256", ""))
        self.similarity = ""
        self.NSFW = ""

    def fieldnames(self):
        return self.__dict__.keys()

    def to_dict(self):
        return self.__dict__

class SimilarInfo:
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.url = str(data.get("URL", ""))
        self.text = str(data.get("TEXT", ""))
        self.similarity = str(data.get("similarity", ""))
        self.NSFW = str(data.get("NSFW", ""))


class Job:
    def __init__(self, pic_dir_name, output_name, job_logger):
        self.pic_dir_name = pic_dir_name
        self.output_name = output_name
        self.infos = []
        self.job_logger = job_logger


    def extract_info(self):
        files = [self.pic_dir_name+'/'+f for f in os.listdir(self.pic_dir_name) if '.json' in f] # ['0000000.json','00000001.json']
        
        infos = []
        for file in files:
            with open(file, 'r', encoding='utf-8') as f:
                try:
                    info = PicInfo(json.load(f))
                    infos.append(info)
                except Exception as eg:
                    print(f'!!!!!!!!ERROR occured during extracting pic info from Json file {file}, type is {eg}')
                    self.job_logger.info(f'!!!!!!!!ERROR occured during extracting pic info from Json file {file}, type is {eg}')
                    
        print("extract pic info success. dir_name={}, info_len={}".format(self.pic_dir_name, len(infos)))
        self.job_logger.info("extract pic info success. dir_name={}, info_len={}".format(self.pic_dir_name, len(infos)))
        self.infos = infos

    def write_to_csv(self):
        if len(self.infos) == 0:
            return
        fieldnames = self.infos[0].fieldnames()
        with open(self.output_name, "w+", encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for info in self.infos:
                writer.writerow(info.to_dict())


def gen_key(url, caption):
    return "{}-{}".format(url, caption)


def collect_similar_infos(similar_file):
    files = [similar_file+'/'+f for f in os.listdir(similar_file) if '.csv' in f]
    similar_by_name = {}
    for file in files:
        print("read csv file. {}".format(file))
        f = open(file, 'r', encoding='utf-8',)
        reader = csv.DictReader(f)
        for line in reader:
            info = SimilarInfo(line)
            key = gen_key(info.url, info.text)
            similar_by_name[key] = info
        f.close()
    return similar_by_name


def collect_similar_infos_df(similar_file):
    files = [similar_file+'/'+f for f in os.listdir(similar_file) if '.csv' in f]
    similar_by_name = {}
    sim_df = pd.DataFrame()
    for file in files:
        print("read csv file. {}".format(file))
        data = pd.read_csv(file)
        sim_df = sim_df.append(data)

        [
            {"URL": ""},
            {"URL": ""}
        ]
    
    sim_df.to_dict('dict') 
    {
        "URL": ""
    }

    return similar_by_name


def append_similar_with_all(similars, infos, apd_logger):
    fount_cnt  = 0
    for k, v in similars.items():
        info = infos.get(k, None)
        if info is None:
            continue
        if info.similarity != "":
            print("duplicate similiarity. url={}, caption={}\n", info.url, info.caption)
            apd_logger.info("duplicate similiarity. url={}, caption={}\n", info.url, info.caption)

        info.similarity = v.similarity
        info.NSFW = v.NSFW
        fount_cnt += 1
        if fount_cnt == len(infos):
            print("read csv, found_cnt={}".format(fount_cnt))
            #logger.info("read csv. {}, file_found_cnt={}".format(file, file_found_cnt))
            break
    if len(infos) == fount_cnt:
        print("append_similar succeeded. info_length={}, fount_cnt={}".format(len(infos), fount_cnt))
        apd_logger.info("append_similar succeeded. info_length={}, fount_cnt={}".format(len(infos), fount_cnt))
    else:
        print("!!ERROR!!! append_similar failed. info_length={}, fount_cnt={}".format(len(infos), fount_cnt))
        apd_logger.info("!!ERROR!!! append_similar failed. info_length={}, fount_cnt={}".format(len(infos), fount_cnt))


def append_similar_break(similar_file, info_by_key, apd_logger):
    fount_cnt = 0
    
    files = [similar_file+'/'+f for f in os.listdir(similar_file) if '.csv' in f]
    files = sorted(files) 
    for file in files:
        file_found_cnt = 0
        with open(file, 'r', encoding='utf-8',) as f:
            reader = csv.DictReader(f)
            for line in reader:
                key = gen_key(line.get("URL"), line.get("TEXT"))
                info = info_by_key.get(key, None)
                if info is None:
                    continue

                # print("info found. {}".format(key))
                if info.similarity != "":
                    print("duplicate similiarity. url={}, caption={}\n", info.url, info.caption)
                    apd_logger.info("duplicate similiarity. url={}, caption={}\n", info.url, info.caption)
                
                info.similarity = line.get("similarity", "")
                info.NSFW = line.get("NSFW", "")
                file_found_cnt += 1
                fount_cnt += 1

        if fount_cnt == len(info_by_key):
            print("read csv. {}, file_found_cnt={}".format(file, file_found_cnt))
            #logger.info("read csv. {}, file_found_cnt={}".format(file, file_found_cnt))
            break
        print("read csv. {}, file_found_cnt={}".format(file, file_found_cnt))
    
    if len(info_by_key) == fount_cnt:
        print("append_similar succeeded. info_length={}, fount_cnt={}".format(len(info_by_key), fount_cnt))
        apd_logger.info("append_similar succeeded. info_length={}, fount_cnt={}".format(len(info_by_key), fount_cnt))
    else:
        print("!!ERROR!!! append_similar failed. info_length={}, fount_cnt={}".format(len(info_by_key), fount_cnt))
        apd_logger.info("!!ERROR!!! append_similar failed. info_length={}, fount_cnt={}".format(len(info_by_key), fount_cnt))
        

def handle_one_folder(img_dir, out_csv_dir, sub_dir_name, sim_dir, index, handle_logger):
    
    print(f'-----The {index}th subfolder: {sub_dir_name}-----')
    #handle_logger.info(f'-----The {index}th subfolder: {sub_dir_name}-----')

    img_dir_name = img_dir + "/" + sub_dir_name
    output_name = out_csv_dir + "/" + sub_dir_name + ".csv"  
    
    job = Job(img_dir_name,  output_name, handle_logger)
    
    #-----------------extract pic json files-----------------
    all_info_by_key = {}
    
    job.extract_info()
    
    for info in job.infos:
        key = gen_key(info.url, info.caption)
        if key in all_info_by_key:
            print("duplicate key={}\n".format(key))
        all_info_by_key[key] = info
                
    info_num = len(all_info_by_key)
                
    #---------extract similarity by url-caption pair-----------
    append_similar_break(sim_dir, all_info_by_key, handle_logger)
    job.write_to_csv()
    return info_num

def handle_one_folder_v2(img_dir, out_csv_dir, sub_dir_name, similars, index, handle_logger):
    
    print(f'-----The {index}th subfolder: {sub_dir_name}-----')
    #handle_logger.info(f'-----The {index}th subfolder: {sub_dir_name}-----')

    img_dir_name = img_dir + "/" + sub_dir_name
    output_name = out_csv_dir + "/" + sub_dir_name + ".csv"  
    
    job = Job(img_dir_name,  output_name, handle_logger)
    
    #-----------------extract pic json files-----------------
    all_info_by_key = {}
    
    job.extract_info()
    
    for info in job.infos:
        key = gen_key(info.url, info.caption)
        if key in all_info_by_key:
            print("duplicate key={}\n".format(key))
        all_info_by_key[key] = info
                
    info_num = len(all_info_by_key)
                
    #---------extract similarity by url-caption pair-----------
    append_similar_with_all(similars, all_info_by_key, handle_logger)
    job.write_to_csv()
    return info_num


if __name__ == "__main__": 
    

    logger = logging.getLogger('laion2b')
    logger.setLevel(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    # log to file
    tname = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    logpath = 'test.log'
    file_handler = logging.FileHandler(logpath)
    file_handler.setLevel(level=logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    pic_dir = "/Users/anyang/Downloads/linser/pic/"       
    similarity_dir = "/Users/anyang/Downloads/linser/csv"
        
    out_dir = "/Users/anyang/Downloads/linser/pic_similar"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    subfolders = [f for f in os.listdir(pic_dir) if not '.' in f] # ['00000','00001']
    subfolders = sorted(subfolders)
    print(f'There are {len(subfolders)} folders in total to be processed.') 

    start_time = time.time()
    #similars = collect_similar_infos("./csv")

    for i in range(len(subfolders)):
        #handle_one_folder_v2(pic_dir, out_dir, subfolders[i], similars, i, logger)
        handle_one_folder(pic_dir, out_dir, subfolders[i], similarity_dir, i, logger)

    end_time = time.time()
    run_time = end_time - start_time
    print(run_time)

    '''
    # script initialization
    parser = argparse.ArgumentParser(description='Extract Img-Cap Similarity From Parquet for Laion2b-en')
    parser.add_argument("--rpath", type=str, default='/jtcv2308ssd-cluster/songlingxue/laion2b-en/', help="")
    parser.add_argument("--spath", type=str, default='/jtcv2308ssd-cluster/songlingxue/laion2b-en/', help="")
    parser.add_argument("--fname", type=str, default='laion2b-en_00024', help="")
    parser.add_argument("--logpath", type=str, default='/jtcv2308ssd-cluster/songlingxue/laion2b-en/logs/extract_similarity_nsfw/', help="")
    parser.add_argument("--resumeid_tp1", type=int, default='0', help="")
    parser.add_argument("--resumeid_tp2_out", type=int, default='0', help="")
    parser.add_argument("--workers", type=int, default='8', help="")
    
    args = parser.parse_args()
    print(args)
    
    csv.field_size_limit(100000000)

    #-------------------------------------log setting------------------------------------
    log_folder = args.logpath
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    logger = logging.getLogger('laion2b')
    logger.setLevel(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    # log to file
    tname = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    logpath = log_folder + args.fname + '_extract_similarity_and_nsfw_from_parquet_' + tname + '.log'
    file_handler = logging.FileHandler(logpath)
    file_handler.setLevel(level=logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(args)

    #--------------------------------------file settings-------------------------------------
    
    FNAMES1 = ["laion2b-en_00000", "laion2b-en_00001", "laion2b-en_00002", "laion2b-en_00003", "laion2b-en_00004", "laion2b-en_00048",
               "laion2b-en_00049", "laion2b-en_00050", "laion2b-en_00051", "laion2b-en_00052", "laion2b-en_00053", "laion2b-en_00054",
               "laion2b-en_00055", "laion2b-en_00056", "laion2b-en_00057", "laion2b-en_00058", "laion2b-en_00059", "laion2b-en_00060",
               "laion2b-en_00061", "laion2b-en_00062", "laion2b-en_00063", "laion2b-en_00064", "laion2b-en_00065", "laion2b-en_00066",
               "laion2b-en_00067", "laion2b-en_00068", "laion2b-en_00069", "laion2b-en_00070", "laion2b-en_00071", "laion2b-en_00072",
               "laion2b-en_00097", "laion2b-en_00098", "laion2b-en_00099", "laion2b-en_00100", "laion2b-en_00101", "laion2b-en_00102",
               "laion2b-en_00103", "laion2b-en_00104", "laion2b-en_00105", "laion2b-en_00106", "laion2b-en_00107", "laion2b-en_00108",
               "laion2b-en_00109", "laion2b-en_00110", "laion2b-en_00111", "laion2b-en_00112", "laion2b-en_00113", "laion2b-en_00114",
               "laion2b-en_00115", "laion2b-en_00116", "laion2b-en_00117", "laion2b-en_00118", "laion2b-en_00119", "laion2b-en_00123",
               "laion2b-en_00124", "laion2b-en_00125", "laion2b-en_00126", "laion2b-en_00127", "laion2b-en_00024", "laion2b-en_00120",
               "laion2b-en_00121", "laion2b-en_00122"
              ]

    if args.fname in FNAMES1:
        # suitable for laion2b-en_0000*/00000
        #-------------------------------------path setting------------------------------------
        pic_dir = args.rpath + args.fname + "/img"       
        similarity_dir = "/jtcv2308hdd-cluster/01-datasets_vlm/05-LAION/laion2b-en/parquet_to_csv/" + args.fname
        
        out_dir = args.spath + "similarity_nfsw/" + args.fname
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        subfolders = [f for f in os.listdir(pic_dir) if not '.' in f] # ['00000','00001']
        subfolders = sorted(subfolders)
        print(f'There are {len(subfolders)} folders in total to be processed in {args.fname}.')

        total_num = 0
        start_time_all = time.time()

        
        futures = []
        #result = []
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
            
            for i in range(args.resumeid_tp1, len(subfolders)):           
                
                futures.append(executor.submit(handle_one_folder, pic_dir, out_dir, subfolders[i], similarity_dir, i, logger))

            for future in concurrent.futures.as_completed(futures):
                #result.append(future.result())  
                total_num += future.result()       
        

        end_time_all = time.time()
        run_time_all = end_time_all - start_time_all
        print(f'-----THE END : {run_time_all}s spent for folder {args.fname}, {total_num} json files in total-----')
        logger.info(f'-----THE END : {run_time_all}s spent for folder {args.fname}, {total_num} json files in total-----')

    else:
        
        # suitable for laion2b-en_0000*/00/img/00000
        #-------------------------------------path setting------------------------------------
        pic_dir = args.rpath + args.fname
              
        subfolders = [f for f in os.listdir(pic_dir) if not '.' in f] # ['00','01']  ['12_01','12_15']
        subfolders = sorted(subfolders)
        print(f'There are {len(subfolders)} folders in total to be processed in {args.fname}.')

        total_num = 0
        start_time_all = time.time()

        #for i in range(len(subfolders)):
        for i in range(args.resumeid_tp2_out, len(subfolders)):
            print(f'********The {i}th subfolder: {subfolders[i]}********') # 00

            pic_sub_dir = pic_dir + "/" + subfolders[i] + "/img"           
            similarity_dir = pic_dir + "/" + subfolders[i]
            
            out_dir = args.spath + "similarity_nfsw/" + args.fname + "/" + subfolders[i]
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            subsubfolders = [f for f in os.listdir(pic_sub_dir) if not '.' in f] # ['00000','00001']
            subsubfolders = sorted(subsubfolders)
            print(f'There are {len(subsubfolders)} subfolders in total to be processed in {pic_sub_dir}.')      

            
            
            futures = []
            #result = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
                
                for j in range(len(subsubfolders)):           
                    
                    futures.append(executor.submit(handle_one_folder, pic_sub_dir, out_dir, subsubfolders[j], similarity_dir, j, logger))

                for future in concurrent.futures.as_completed(futures):
                    #result.append(future.result())
                    total_num += future.result()      
        
            
        end_time_all = time.time()
        run_time_all = end_time_all - start_time_all
        print(f'********THE END : {run_time_all}s spent for folder {args.fname}, {total_num} json files in total********')
        logger.info(f'********THE END : {run_time_all}s spent for folder {args.fname}, {total_num} json files in total********')

    '''            
