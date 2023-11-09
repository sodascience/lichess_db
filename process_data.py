from multiprocessing import Pool, freeze_support
from lichess_ingester import read_and_process

def main():
    freeze_support()
    pq_dir = "lichess_parquet_raw"
    years = [2014, 2015]

    i = 0
    arguments = [(y, m + 1, pq_dir) for y in years for m in range(12)]
    arguments = [arg + (i,) for i, arg in enumerate(arguments)]   

    with Pool(4) as p:
        p.starmap(read_and_process, arguments)


if __name__ == "__main__":
    main()