#####################################################################
# important! I decompressed all the data files to a folder I named
# supermarketdata. If you don't do that as well you will have some errors!!!
#####################################################################

import pandas as pd # Don't forget to install before running!
import matplotlib # Don't forget to install before running!
import natsort # Don't forget to install before running!
import gzip
import shutil
import os
import xml.etree.ElementTree as Xet


DIRECTORIES = ["spermarketdata/hashook",
               "spermarketdata/keshet", "spermarketdata/mega",
               "spermarketdata/osherad", "spermarketdata/ramilevi",
               "spermarketdata/tivtaam", "spermarketdata/victory"]
STORES_DIRECTORIES = ["spermarketdata/tivtaam", "spermarketdata/ramilevi",
                      "spermarketdata/osherad", "spermarketdata/keshet"]
PROMO_DIRECTORIES = []
STORES_COLS = ["ChainName", "StoreId", "BikoretNo", "StoreType", "StoreName",
               "Address", "City"]
STORES_FULL_COLS = ["ChainID", "ChainName", "StoreID", "SubChainName",
                  "StoreName", "Address", "City"]
PROMO_COLS = ["StoreId", "Promotions Count", "PromotionId",
              "PromotionDescription", "PromotionStartDate",
              "PromotionStartHour", "PromotionEndDate", "PromotionEndHour",
              "MinQty", "DiscountedPrice", "DiscountedPricePerMida"]
PROMO_FULL_COLS = ["ChainID", "StoreID", "ItemCode",
              "PromotionID", "PromotionDescription",
              "PromotionDescription2", "PromotionStartDate",
              "PromotionStartHour", "PromotionEndDate", "PromotionEndHour",
              "MinPurchaseAmount", "DiscountedPrice", "DiscountedPricePerMida"]
PROMO_COLS_VER2 = ["ChainID", "StoreID", "ItemCode",
              "PromotionID", "PromotionDescription", "PromotionStartDate",
              "PromotionStartHour", "PromotionEndDate", "PromotionEndHour",
              "MinQty", "DiscountedPrice", "DiscountedPricePerMida"]
PRICE_COLS = ["ItemName", "ManufactureName", "UnitQty", "Quantity",
             "QtyInPackage", "ItemPrice", "UnitOfMeasurePrice"]


def invalid_file_formats(directories):
    """
    deleting all invalid file formats from the different folders
    :param directories: directory list
    :return: no return value
    """
    for directory in directories:
        files_in_directory = os.listdir(directory)
        filtered_files = [file for file in files_in_directory if
                          file.endswith(".gz.items") or
                          file.endswith(".xml.items") or
                          file.endswith(".xml.gz.items")]
        for file in filtered_files:
            path_to_file = os.path.join(directory, file)
            os.remove(path_to_file)


def extract_gz(directories):
    """
    attempting to extract files in a list of directories if the file is not
    corrupted. Deleting the gz compressed file afterwards.
    :param directories: the directories with gz files suspected as corrupted.
    :return: a list with all the corrupted file names.
    """
    corrupted_files = []
    for directory in directories:
        files_in_directory = os.listdir(directory)
        for file in files_in_directory:
            if file.endswith(".gz"):
                full_path = directory + "/" + file
                try:
                    with gzip.open(full_path, 'rb') as f_in:
                        with open(full_path[:-3] + ".xml", 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                # handling the file if corrupted
                except OSError:
                    corrupted_files.append(file)
                # deleting the original gz file
                os.remove(full_path)
    return corrupted_files


def convert_stores_xml_to_csv(directories):
    """
    convert xml files to cvs files with the same name to transfer the data
    to a more "manipulation-friendly" format.
    :param directories: directories with data xml files
    :return: no return value, new csv files are created in the same folders
    of the original xml files.
    """
    for directory in directories:
        files_in_directory = os.listdir(directory)
        for file in files_in_directory:
            if file.startswith("Stores"):
                try:
                    full_path = directory + "/" + file
                    cols = STORES_COLS
                    rows = []
                    # Parsing the XML file
                    xmlparse = Xet.parse(full_path)
                    root = xmlparse.getroot()
                    chain_name = root.find("ChainName").text
                    # going in the relevant tag withing the xml tree and extracting
                    # the data
                    for subchains in root:
                        for subchain in subchains:
                            for stores in subchain:
                                for store in stores:
                                    store_id = store.find("StoreId").text
                                    bikoret_no = store.find("BikoretNo").text
                                    store_type = store.find("StoreType").text
                                    store_name = store.find("StoreName").text
                                    address = store.find("Address").text
                                    city = store.find("City").text

                                    rows.append({"ChainName": chain_name,
                                                 "StoreId": store_id,
                                                 "BikoretNo": bikoret_no,
                                                 "StoreType": store_type,
                                                 "StoreName": store_name,
                                                 "Address": address,
                                                 "City": city})

                    df = pd.DataFrame(rows, columns=cols)
                    # Writing dataframe to csv
                    df.to_csv(full_path[:-3] + "csv")
                    os.remove(full_path)
                except:
                    continue


def convert_stores_full_xml_to_csv(directories):
    """
    convert xml files to cvs files with the same name to transfer the data
    to a more "manipulation-friendly" format.
    :param directories: directories with data xml files
    :return: no return value, new csv files are created in the same folders
    of the original xml files.
    """
    for directory in directories:
        files_in_directory = os.listdir(directory)
        for file in files_in_directory:
            if file.startswith("StoresFull"):
                full_path = directory + "/" + file
                cols = ["ChainID", "ChainName", "StoreID", "SubChainName",
                  "StoreName", "Address", "City"]
                rows = []
                # Parsing the XML file
                xmlparse = Xet.parse(full_path)
                root = xmlparse.getroot()
                # going in the relevant tag withing the xml tree and extracting
                # the data
                for branches in root:
                    for branch in branches:
                        chain_id = branch.find("ChainID").text
                        chain_name = branch.find("ChainName").text
                        store_id = branch.find("StoreID").text
                        sub_chain_name = branch.find("SubChainName").text
                        store_name = branch.find("StoreName").text
                        address = branch.find("Address").text
                        city = branch.find("City").text

                        rows.append({"ChainID": chain_id,
                                     "ChainName": chain_name,
                                     "StoreID": store_id,
                                     "SubChainName": sub_chain_name,
                                     "StoreName": store_name,
                                     "Address": address,
                                     "City": city})

                df = pd.DataFrame(rows, columns=cols)
                # Writing dataframe to csv
                df.to_csv(full_path[:-3] + "csv")
                os.remove(full_path)


def convert_promo_xml_to_csv(directories):
    for directory in directories:
        files_in_directory = os.listdir(directory)
        for file in files_in_directory:
            if file.startswith("Promo"):
                full_path = directory + "/" + file
                cols = PROMO_COLS
                rows = []
                # Parsing the XML file
                xmlparse = Xet.parse(full_path)
                root = xmlparse.getroot()
                store_id = root.find("StoreId").text
                # space in "promotions count" tag! refer only to first word
                promotions_count = root.find("Promotions").attrib["Count"]
                # going in the relevant tag withing the xml tree and extracting
                # the data
                for promotions in root:
                    for promotion in promotions:
                        promotion_id = promotion.find("PromotionId").text
                        promotion_description = promotion.find(
                            "PromotionDescription").text
                        start_date = promotion.find(
                            "PromotionStartDate").text
                        start_hour = promotion.find(
                            "PromotionStartHour").text
                        end_date = promotion.find("PromotionEndDate").text
                        end_hour = promotion.find("PromotionEndHour").text
                        min_qty = promotion.find("MinQty").text
                        if promotion.find("DiscountedPrice") is not None:
                            discounted_price = promotion.find(
                                "DiscountedPrice").text
                            discounted_price_mida = promotion.find(
                                "DiscountedPricePerMida").text
                        else:
                            discounted_price = "Nan"
                            discounted_price_mida = "Nan"

                        rows.append({"StoreId": store_id,
                                     "Promotions Count": promotions_count,
                                     "PromotionId": promotion_id,
                                     "PromotionDescription":
                                         promotion_description,
                                     "PromotionStartDate": start_date,
                                     "PromotionStartHour": start_hour,
                                     "PromotionEndDate": end_date,
                                     "PromotionEndHour": end_hour,
                                     "MinQty": min_qty,
                                     "DiscountedPrice": discounted_price,
                                     "DiscountedPricePerMida":
                                         discounted_price_mida
                                     })

                df = pd.DataFrame(rows, columns=cols)
                # Writing dataframe to csv
                df.to_csv(full_path[:-3] + "csv")
                os.remove(full_path)


def convert_promo_xml_to_csv_ver2(directories):
    for directory in directories:
        files_in_directory = os.listdir(directory)
        for file in files_in_directory:
            if file.startswith("Promo"):
                full_path = directory + "/" + file
                cols = PROMO_COLS_VER2
                rows = []
                # Parsing the XML file
                xmlparse = Xet.parse(full_path)
                root = xmlparse.getroot()
                store_id = root.find("StoreID").text
                chain_id = root.find("ChainID").text
                # going in the relevant tag withing the xml tree and extracting
                # the data
                for sales in root:
                    for sale in sales:
                        item_code = sale.find("ItemCode").text
                        promotion_id = sale.find("PromotionID").text
                        promotion_description = sale.find(
                            "PromotionDescription").text
                        start_date = sale.find(
                            "PromotionStartDate").text
                        start_hour = sale.find(
                            "PromotionStartHour").text
                        end_date = sale.find("PromotionEndDate").text
                        end_hour = sale.find("PromotionEndHour").text
                        min_qty = sale.find("MinQty").text
                        if sale.find("DiscountedPrice") is not None:
                            discounted_price = sale.find(
                                "DiscountedPrice").text
                            discounted_price_mida = sale.find(
                                "DiscountedPricePerMida").text
                        else:
                            discounted_price = "Nan"
                            discounted_price_mida = "Nan"

                        rows.append({"ChainID": chain_id,
                                     "StoreId": store_id,
                                     "ItemCode": item_code,
                                     "PromotionId": promotion_id,
                                     "PromotionDescription":
                                         promotion_description,
                                     "PromotionStartDate": start_date,
                                     "PromotionStartHour": start_hour,
                                     "PromotionEndDate": end_date,
                                     "PromotionEndHour": end_hour,
                                     "MinQty": min_qty,
                                     "DiscountedPrice": discounted_price,
                                     "DiscountedPricePerMida":
                                         discounted_price_mida
                                     })

                df = pd.DataFrame(rows, columns=cols)
                # Writing dataframe to csv
                df.to_csv(full_path[:-3] + "csv")
                os.remove(full_path)


def convert_promo_full_xml_to_csv(directories):
    for directory in directories:
        files_in_directory = os.listdir(directory)
        for file in files_in_directory:
            if file.startswith("PromoFull"):
                full_path = directory + "/" + file
                cols = PROMO_FULL_COLS
                rows = []
                # Parsing the XML file
                xmlparse = Xet.parse(full_path)
                root = xmlparse.getroot()
                chain_id = root.find("ChainID").text
                store_id = root.find("StoreID").text
                # going in the relevant tag withing the xml tree and extracting
                # the data
                for sales in root:
                    for sale in sales:
                        item_code = sale.find("ItemCode").text
                        promotion_id = sale.find("PromotionID").text
                        promotion_description = sale.find(
                            "PromotionDescription").text
                        if sale.find("PromotionDescription2") is not None:
                            promotion_description2 = sale.find(
                                "PromotionDescription2").text
                        else:
                            promotion_description2 = "None"
                        start_date = sale.find(
                            "PromotionStartDate").text
                        start_hour = sale.find(
                            "PromotionStartHour").text
                        end_date = sale.find("PromotionEndDate").text
                        end_hour = sale.find("PromotionEndHour").text
                        min_amount = sale.find("MinPurchaseAmount").text
                        if sale.find("DiscountedPrice") is not None:
                            discounted_price = sale.find(
                                "DiscountedPrice").text
                            discounted_price_mida = sale.find(
                                "DiscountedPricePerMida").text
                        else:
                            discounted_price = "Nan"
                            discounted_price_mida = "Nan"

                        rows.append({"ChainID": chain_id,
                                     "StoreID": store_id,
                                     "ItemCode": item_code,
                                     "PromotionID": promotion_id,
                                     "PromotionDescription":
                                         promotion_description,
                                     "PromotionDescription2":
                                         promotion_description2,
                                     "PromotionStartDate": start_date,
                                     "PromotionStartHour": start_hour,
                                     "PromotionEndDate": end_date,
                                     "PromotionEndHour": end_hour,
                                     "MinPurchaseAmount": min_amount,
                                     "DiscountedPrice": discounted_price,
                                     "DiscountedPricePerMida":
                                         discounted_price_mida
                                     })


                df = pd.DataFrame(rows, columns=cols)
                # Writing dataframe to csv
                df.to_csv(full_path[:-3] + "csv")
                os.remove(full_path)


def convert_price_xml_to_csv(directories):
    for directory in directories:
        files_in_directory = os.listdir(directory)
        for file in files_in_directory:
            if file.startswith("Price"):
                full_path = directory + "/" + file
                cols = PRICE_COLS
                rows = []
                # Parsing the XML file
                xmlparse = Xet.parse(full_path)
                root = xmlparse.getroot()
                store_id = root.find("StoreID").text
                # going in the relevant tag withing the xml tree and extracting
                # the data
                for products in root:
                    for product in products:
                        item_name = product.find("ItemName").text
                        manufacture_name = product.find("ManufactureName").text
                        unit_qty = product.find("UnitQty").text
                        qty_in_package = product.find("QtyInPackage").text
                        item_price = product.find("ItemPrice").text
                        unit_of_measure_price = product.find(
                            "UnitOfMeasurePrice").text

                        rows.append({"StoreID": store_id,
                                     "ItemName": item_name,
                                     "ManufactureName": manufacture_name,
                                     "UnitQty": unit_qty,
                                     "QtyInPackage": qty_in_package,
                                     "ItemPrice": item_price,
                                     "UnitOfMeasurePrice":
                                         unit_of_measure_price})

                df = pd.DataFrame(rows, columns=cols)
                # Writing dataframe to csv
                df.to_csv(full_path[:-3] + "csv")
                os.remove(full_path)


def extract_branch_IDs(directory):
    branches_lst = []
    files_to_visualize = os.listdir(directory)
    for file in files_to_visualize:
        data = pd.read_csv(directory + "/" + file)
        relevant_data = data.StoreID  # not general, refers only to Date called column
        for line in relevant_data:
            if line not in branches_lst:
                branches_lst.append(line)
    return branches_lst


def extract_specific_product_price(directory, product, branch_lst):
    files_to_visualize = os.listdir(directory)
    for file in files_to_visualize:
        data = pd.read_csv(directory + "/" + file)
        for line in data.PromotionDescription:
            if product in line:
                print(line)


if __name__ == "__main__":
    # pd.set_option('display.max_columns', None)
    folder_visualize = "spermarketdata\Visualize"
    # print(visualize1(folder_visualize))
    branch_lst = extract_branch_IDs(folder_visualize)
    extract_specific_product_price(folder_visualize, "בירה", branch_lst)