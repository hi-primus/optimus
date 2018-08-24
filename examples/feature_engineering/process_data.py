import featuretools as ft
import pandas as pd
import os
from tqdm import tqdm


def make_user_sample(orders, order_products, departments, products, user_ids, out_dir):
    orders_sample = orders[orders["user_id"].isin(user_ids)]

    orders_keep = orders_sample["order_id"].values
    order_products_sample = order_products[order_products["order_id"].isin(orders_keep)]

    try:
        os.mkdir(out_dir)
    except:
        pass
    order_products_sample.to_csv(os.path.join(out_dir, "order_products__prior.csv"), index=None)
    orders_sample.to_csv(os.path.join(out_dir, "orders.csv"), index=None)
    departments.to_csv(os.path.join(out_dir, "departments.csv"), index=None)
    products.to_csv(os.path.join(out_dir, "products.csv"), index=None)


def main():
    data_dir = "data"
    order_products = pd.concat([pd.read_csv(os.path.join(data_dir,"order_products__prior.csv")),
                                pd.read_csv(os.path.join(data_dir, "order_products__train.csv"))])
    orders = pd.read_csv(os.path.join(data_dir, "orders.csv"))
    departments = pd.read_csv(os.path.join(data_dir, "departments.csv"))
    products = pd.read_csv(os.path.join(data_dir, "products.csv"))

    users_unique = orders["user_id"].unique()
    chunksize = 1000
    part_num = 0
    partition_dir = "partitioned_data"
    try:
        os.mkdir(partition_dir)
    except:
        pass
    for i in tqdm(range(0, len(users_unique), chunksize)):
        users_keep = users_unique[i: i+chunksize]
        make_user_sample(orders, order_products, departments, products, users_keep, os.path.join(partition_dir, "part_%d" % part_num))
        part_num += 1

if __name__ == "__main__":
    main()
