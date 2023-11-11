from lichess_ingester import read_and_process

def main():
    pq_dir = "lichess_parquet"
    years = [2016, 2017, 2018]

    i = 0
    arguments = [(y, m + 1, pq_dir) for y in years for m in range(12)]

    for a in arguments:
        read_and_process(*a)


if __name__ == "__main__":
    main()