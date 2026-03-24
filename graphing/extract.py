import json

def filter_and_sort(
    path,
    engine_name,
    dataset_name,
    parallel,
    min_mean_precisions,
    sort_metric="rps",
    descending=True,
):
    with open(path, "r") as f:
        data = json.load(f)

    filtered = [
        row for row in data
        if row.get("engine_name") == engine_name
        and row.get("dataset_name") == dataset_name
        and row.get("parallel") == parallel
        and row.get("mean_precisions", 0) >= min_mean_precisions
        and not (
            engine_name == "qdrant"
            and (row.get("engine_params") or {}).get("quantization") is not None
        )
    ]

    # Sort, treating missing/None metric as -inf so they go to the end
    def sort_key(row):
        val = row.get(sort_metric)
        return float("-inf") if val is None else val

    filtered.sort(key=sort_key, reverse=descending)
    return filtered


target_dataset = "gist-960-euclidean"
target_dataset = "dbpedia-openai-1M-1536-angular"
# target_dataset = "glove-100-angular"
# target_dataset = "deep-image-96-angular"
# Example usage
# engines = ["milvus", "qdrant", "weaviate"]
engines = [ "qdrant", "weaviate"]
# engines = ["weaviate"]
for engine in engines:
    results = filter_and_sort(
        path="cloud_exp.json",
        engine_name=engine,
        dataset_name=target_dataset,
        parallel=100,
        min_mean_precisions=0.95,
        sort_metric="rps",
    )

    print(f"{engine}: Found {len(results)} rows")
    print(target_dataset)
    for row in results[:1]:
        print(row)
        # print(
        #     f"setup_name: {row['setup_name']}",
        #     f"engine_params: {row['engine_params']}",
        #     f"upload: {row['upload_time']}",
        #     f"index: {row['total_upload_time'] - row['upload_time']}",
        #     f"mean_precisions: {row['mean_precisions']}",
        #     f"rps: {row['rps']}",
        #     f"latency: {row['mean_time'] * 1000}",
        #     # f"total: {row['total_time']}",
        # )
    break
