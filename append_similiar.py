# coding=utf-8
import os
import os.path
import json
import csv

class PicInfo:
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.caption = str(data.get("caption", ""))
        self.url = str(data.get("url", ""))
        self.key = str(data.get("key", ""))
        self.status = str(data.get("status", ""))
        self.error_message = str(data.get("error_message", ""))
        self.width = str(data.get("width", ""))
        self.height = str(data.get("height", ""))
        self.original_width = str(data.get("original_width", ""))
        self.original_height = str(data.get("original_height", ""))
        self.exif = str(data.get("exif", ""))
        self.sha256 = str(data.get("sha256", ""))
        self.similarity = ""

    def fieldnames(self):
        return self.__dict__.keys()

    def to_dict(self):
        return self.__dict__

class Job:
    def __init__(self, pic_dir_name, output_name, csv_file=None):
        self.pic_dir_name = pic_dir_name
        self.output_name = output_name
        self.infos = []
        self.csv_file = csv_file

    def extract_info_from_csv(self):
        if self.csv_file is None:
            return
        infos = []
        with open(self.csv_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for line in reader:
                infos.append(PicInfo(line))
        self.infos = infos

    def extract_info(self):
        files = list_all_files_with_suffix(self.pic_dir_name, "json")
        infos = []
        for file in files:
            with open(file, 'r') as f:
                content = f.read()
                info = PicInfo(json.loads(content))
                infos.append(info)
        print("extract pic info success. dir_name={}, info_len={}\n".format(self.pic_dir_name, len(infos)))
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

def list_all_files_with_suffix(dir_name, suffix):
    files = []
    for parent, sub_dir_names, file_names in os.walk(dir_name):
        for file_name in file_names:
            if file_name.endswith(suffix):
                files.append(os.path.join(parent, file_name))
    return files

def append_similar(similar_file, info_by_key):
    fount_cnt = 0
    files = list_all_files_with_suffix(similar_file, "csv")
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
                    print("dumplicate similiarity. url={}, caption={}\n", info.url, info.caption)
                info.similarity = line.get("similarity", "")
                file_found_cnt += 1
                fount_cnt += 1
        print("read csv.{}, file_found_cnt={}\n".format(file, file_found_cnt))
    print("append_similar success. info_length={}, fount_cnt={}".format(len(info_by_key), fount_cnt))


def demo_read_from_csv(jobs):
    for job in jobs:
        job.csv_file = job.output_name  # 可以一开始就赋值
        job.extract_info_from_csv()


def demo_extract_to_csv(jobs):
    for job in jobs:
        job.extract_info()
        job.write_to_csv()


if __name__ == "__main__":
    jobs = [
        Job(pic_dir_name="./pic/00000",  output_name="./pic_similar/00000.csv"),
        Job(pic_dir_name="./pic/00001",  output_name="./pic_similar/00001.csv"),
        Job(pic_dir_name="./pic/00002",  output_name="./pic_similar/00002.csv"),
        Job(pic_dir_name="./pic/00003",  output_name="./pic_similar/00003.csv"),
        Job(pic_dir_name="./pic/00004",  output_name="./pic_similar/00004.csv"),
        Job(pic_dir_name="./pic/00005",  output_name="./pic_similar/00005.csv"),
        Job(pic_dir_name="./pic/00006",  output_name="./pic_similar/00006.csv"),
        Job(pic_dir_name="./pic/00007",  output_name="./pic_similar/00007.csv"),
        Job(pic_dir_name="./pic/00008",  output_name="./pic_similar/00008.csv"),
        Job(pic_dir_name="./pic/00009",  output_name="./pic_similar/00009.csv"),
    ]
    similarity_dir = "./csv"

    all_info_by_key = {}
    for job in jobs:
        job.extract_info()
        for info in job.infos:
            key = gen_key(info.url, info.caption)
            if key in all_info_by_key:
                print("duplicate key={}\n".format(key))
            all_info_by_key[key] = info
    
    append_similar(similarity_dir, all_info_by_key)
    for job in jobs:
        job.write_to_csv()

    