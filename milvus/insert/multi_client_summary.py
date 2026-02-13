import numpy as np
import csv
import os
from datetime import datetime

def inspect_npy(path, ag_time):
    arr = np.load(path)
    print(f"File: {path}")
    print(f"shape: {arr.shape}")

    # Print first element safely based on dimension
    try:
        first = arr.flat[0]   # Works for any shape
        print(f"First element: {first}")
    except Exception as e:
        print(f"Could not get first element: {e}")

    print()



def summarize_npy(path, rank, name, batch_size):
    arr = np.load(path) * 1000

    return [rank, name, np.sum(arr),np.mean(arr),np.std(arr), np.percentile(arr, 99), len(arr) / (np.sum(arr)/ 1000), (batch_size * len(arr)) / (np.sum(arr) / 1000)], arr

def ag_stats(rank, name, arr, totalTime,batch_size):
    return [rank, name, np.sum(arr),np.mean(arr),np.std(arr), np.percentile(arr, 99), len(arr) / (totalTime), (batch_size * len(arr)) / (totalTime)]

def extract_time(rank):
    with open("times.csv", newline="") as f:
        reader = csv.reader(f)
         
        for row in reader:
            if row[0] == f"{rank}":
                targetRow = row
                break
    # start = datetime.fromisoformat(targetRow[1])
    # end = datetime.fromisoformat(targetRow[2])
    # globalEnd = datetime.fromisoformat(targetRow[3])

    return float(targetRow[8])


   

# Example usage:
# summarize_npy("target/debug/upload_times.npy")
# summarize_npy("target/debug/op_times.npy")

batch_size = int(os.getenv("UPLOAD_BATCH_SIZE"))
clients = int(os.getenv("N_WORKERS"))
cPerWorker = int(os.getenv("UPLOAD_CLIENTS_PER_WORKER"))

# clients = clients * cPerWorker

# batch_size = 128
# clients = 2

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
    writer.writerow(ag_stats("all","prep",stacked_prep, ag_time, batch_size))
    writer.writerow(ag_stats("all","upload",stacked_upload, ag_time, batch_size))
    writer.writerow(ag_stats("all","op",stacked_op, ag_time, batch_size))
