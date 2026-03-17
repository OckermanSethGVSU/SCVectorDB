import numpy as np
import csv
import os
from datetime import datetime



def summarize_npy(path, rank, name, batch_size):
    arr = np.load(path) * 1000

    return [rank, name, np.sum(arr),np.mean(arr),np.std(arr), np.percentile(arr, 99), len(arr) / (np.sum(arr)/ 1000), (batch_size * len(arr)) / (np.sum(arr) / 1000)], arr

def ag_stats(rank, name, arr, totalTime,CORPUS_SIZE):
    return [rank, name, np.sum(arr),np.mean(arr),np.std(arr), np.percentile(arr, 99), len(arr) / (totalTime), (CORPUS_SIZE) / (totalTime)]

def extract_time(rank):
    with open("times.csv", newline="") as f:
        reader = csv.reader(f)
         
        for row in reader:
            if row[0] == f"{rank}":
                targetRow = row
                break

    return float(targetRow[10])


ACTIVE_TASK = os.getenv("ACTIVE_TASK")
batch_size = int(os.getenv(f"{ACTIVE_TASK}_BATCH_SIZE"))
cPerWorker = int(os.getenv(f"{ACTIVE_TASK}_CLIENTS_PER_PROXY"))
clients = int(os.getenv("NUM_PROXIES"))
CORPUS_SIZE = int(os.getenv(f"{ACTIVE_TASK}_CORPUS_SIZE"))



with open(f"summary.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["rank","operation",'total', 'mean', 'std', "p99","rank_op/s","rank_v/s"])

all_prep = []
all_upload = []
all_op = []
for i in range(clients):
    for j in range(cPerWorker):
        prep, pArr = summarize_npy(f"batch_construction_times_w{i}_c{j}.npy",i, "prep",batch_size)
        upload, uArr = summarize_npy(f"upload_times_w{i}_c{j}.npy",i, "upload",batch_size)
        op, opArr = summarize_npy(f"op_times_w{i}_c{j}.npy",i, "op",batch_size)
        with open(f"summary.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(prep)
            writer.writerow(upload)
            writer.writerow(op)
        all_prep.append(pArr)
        all_upload.append(uArr)
        all_op.append(opArr)

stacked_prep = np.concatenate(all_prep)
stacked_upload = np.concatenate(all_upload)
stacked_op = np.concatenate(all_op)
ag_time = extract_time(0)


with open(f"summary.csv", "a", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(ag_stats("all","prep",stacked_prep, ag_time, CORPUS_SIZE))
    writer.writerow(ag_stats("all","upload",stacked_upload, ag_time, CORPUS_SIZE))
    writer.writerow(ag_stats("all","op",stacked_op, ag_time, CORPUS_SIZE))
