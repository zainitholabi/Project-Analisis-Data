class DataAnalyzer:
    def __init__(self, df):
        self.df = df

    def createDataFrameDailyOrders(self):
        dataFrameDailyOrders = self.df.resample(rule='D', on='order_approved_at').agg({
            "order_id": "nunique",
            "payment_value": "sum"
        })
        dataFrameDailyOrders = dataFrameDailyOrders.reset_index()
        dataFrameDailyOrders.rename(columns={
            "order_id": "order_count",
            "payment_value": "revenue"
        }, inplace=True)
        
        return dataFrameDailyOrders
    
    def createDataFrameSumSpend(self):
        dataFrameSumSpend = self.df.resample(rule='D', on='order_approved_at').agg({
            "payment_value": "sum"
        })
        dataFrameSumSpend = dataFrameSumSpend.reset_index()
        dataFrameSumSpend.rename(columns={
            "payment_value": "total_spend"
        }, inplace=True)

        return dataFrameSumSpend

    def createDataFrameSumOrderItems(self):
        dataFrameSumOrderItems = self.df.groupby("product_category_name_english")["product_id"].count().reset_index()
        dataFrameSumOrderItems.rename(columns={
            "product_id": "product_count"
        }, inplace=True)
        dataFrameSumOrderItems = dataFrameSumOrderItems.sort_values(by='product_count', ascending=False)

        return dataFrameSumOrderItems

    def dataFrameReviewScore(self):
        reviewScore = self.df['review_score'].value_counts().sort_values(ascending=False)
        most_common_score = reviewScore.idxmax()

        return reviewScore, most_common_score

    def createDataFrameBystate(self):
        dataFrameBystate = self.df.groupby(by="customer_state").customer_id.nunique().reset_index()
        dataFrameBystate.rename(columns={
            "customer_id": "customer_count"
        }, inplace=True)
        most_common_state = dataFrameBystate.loc[dataFrameBystate['customer_count'].idxmax(), 'customer_state']
        dataFrameBystate = dataFrameBystate.sort_values(by='customer_count', ascending=False)

        return dataFrameBystate, most_common_state

    def createOrderStatus(self):
        dataFrameOrderStatus = self.df["order_status"].value_counts().sort_values(ascending=False)
        most_common_status = dataFrameOrderStatus.idxmax()

        return dataFrameOrderStatus, most_common_status
    
class BrazilMapPlotter:
    def __init__(self, data, plt, mpimg, urllib, st):
        self.data = data
        self.plt = plt
        self.mpimg = mpimg
        self.urllib = urllib
        self.st = st

    def plot(self):
        brazil = self.mpimg.imread(self.urllib.request.urlopen('https://i.pinimg.com/originals/3a/0c/e1/3a0ce18b3c842748c255bc0aa445ad41.jpg'),'jpg')
        ax = self.data.plot(kind="scatter", x="geolocation_lng", y="geolocation_lat", figsize=(10,10), alpha=0.3,s=0.3,c='maroon')
        self.plt.axis('off')
        self.plt.imshow(brazil, extent=[-73.98283055, -33.8,-33.75116944,5.4])
        self.st.pyplot()
