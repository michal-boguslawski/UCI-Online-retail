import pandas as pd

# write on delivery
def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
#get data
def get_data():
    dtypes_dict = {
            "InvoiceNo": str,
            "StockCode": str,
            "Description": str,
            "Quantity": str,
            "UnitPrice": str,
            "CustomerID": str, 
            "Country": str
        }
    file_name = "Online Retail.xlsx"
    sheet_name = "Online Retail"
    
    df = pd.read_excel(
        file_name,
        sheet_name=sheet_name,
        dtype=dtypes_dict,
        parse_dates=["InvoiceDate"]
    )
    df = df.sort_values(
        by=["InvoiceDate", "InvoiceNo"], 
        ascending=True
    ).reset_index(
        drop=True
    )
    df['InvoiceDate'] = df['InvoiceDate'].astype(str)
    
    for _, row in df.iterrows():
        yield row.to_dict()
        
if __name__ == "__main__":
    df = get_data()
    for _ in range(5):
        print(next(df))
    