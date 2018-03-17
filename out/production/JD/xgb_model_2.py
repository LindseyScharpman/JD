import xgboost as xgb
import pandas as pd

select_days = "75"
path = "H:/JingDong/jd/JData/"


def get_feature():
    feature_list = []
    feature_file = open(path + "lgb_feature_score_2.csv", mode='r')
    i = 0
    for line in feature_file.readlines():
        if i > 0 and int(line.split(",")[1]) > 10:
            feature_list.append(line.split(",")[0])
        i += 1
    feature_file.close()
    return feature_list

item_features = pd.read_csv(path + "item_features_" + select_days + "d.csv")
brand_features = pd.read_csv(path + "brand_features_" + select_days + "d.csv")
ui_features = pd.DataFrame()
for i in range(0, 10):
    tmp = pd.read_csv(path + "splits/useritem_features_" + str(i) + "_" + select_days + "d.csv")
    tmp = tmp[(tmp.which <= 5)]
    ui_features = ui_features.append(tmp)
print(ui_features[(ui_features.label == 1) & (ui_features.which >= 1)].shape[0])
print(ui_features[(ui_features.label == 0) & (ui_features.which >= 1)].shape[0])
uf = pd.read_csv(path + "user_features_" + select_days + "d.csv")
puf = pd.DataFrame()
for i in range(0, 10):
    tmp = pd.read_csv(path + "splits/user_features_" + str(i) + "_" + select_days + "d.csv")
    tmp.drop(['label'], axis=1, inplace=True)
    puf = puf.append(tmp)
ubf = pd.DataFrame()
for i in range(0, 10):
    tmp = pd.read_csv(path + "splits/userbrand_features_" + str(i) + "_" + select_days + "d.csv")
    ubf = ubf.append(tmp)
product_count = pd.DataFrame()
for i in range(0, 10):
    tmp = pd.read_csv(path + "splits/user_count_features_" + str(i) + "_" + select_days + "d.csv")
    product_count = product_count.append(tmp)
data = pd.merge(ui_features, item_features, on=['item', 'which'], how='inner')
data = pd.merge(data, uf, on=['user', 'which'], how='inner')
data = pd.merge(data, product_count, on=['user', 'which'], how='inner')
data = pd.merge(data, puf, on=['user', 'which'], how='inner')
data = pd.merge(data, ubf, on=['user', 'brand', 'which'], how='inner')
data = pd.merge(data, brand_features, on=['brand', 'which'], how='inner')

data['f_new_1'] = (data.product_count_f_1_1 + data.product_count_f_6_1) / (
    data.product_count_f_1_4 + data.product_count_f_6_4 + 0.01)
data['f_new_2'] = (data.product_f_1_1 + data.product_f_6_1) / (
    data.product_f_1_4 + data.product_f_6_4 + 0.01)
data['f_new_3'] = (data.uif_1_1 + data.uif_6_1) / (data.product_f_1_1 + data.product_f_6_1 + 0.01)
data['f_new_4'] = (data.uif_1_1 + data.uif_6_1) / (data.ubf_1_1 + data.ubf_6_1 + 0.01)
data['f_new_5'] = (data.uif_1_4 + data.uif_6_4) / (data.product_f_1_4 + data.product_f_6_4 + 0.01)
data['f_new_6'] = (data.uif_1_4 + data.uif_6_4) / (data.ubf_1_4 + data.ubf_6_4 + 0.01)
data['f_new_7'] = data.product_uf_online_days / data.onlinedays_4
data['f_new_8'] = (data.product_f_1_4 + data.product_f_6_4) / data.product_uf_online_days
data['f_new_9'] = (data.product_brand_f_1_4 + data.product_brand_f_6_4) / data.product_uf_online_days
data['f_new_10'] = (data.product_count_f_1_4 + data.product_count_f_6_4) / data.product_uf_online_days
# data['f_new_11'] = (data.product_count_f_1_4 + data.product_count_f_6_4) / (
#     data.product_f_1_4 + data.product_f_6_4 + 0.01)
data['f_new_11'] = (data.item_f_1_4 + data.item_f_6_4) / (
    data.brand_f_1_4 + data.brand_f_6_4 + 0.01)
data['f_new_12'] = data.item_f_4_4 / (data.brand_f_4_4 + 0.01)
data['f_new_13'] = data.uif_online_days / data.product_uf_online_days
# data['f_rank_1'] = data['uif_online_days'].groupby(data['user']).rank(method='dense', ascending=False)
# data['f_rank_2'] = data['item_f_4_4'].groupby(data['user']).rank(method='dense', ascending=False)
# data['f_rank_3'] = data['uif_1_4'].groupby(data['user']).rank(method='dense', ascending=False)
# data['f_rank_4'] = data['uif_6_4'].groupby(data['user']).rank(method='dense', ascending=False)
# data['f_rank_5'] = data['uif_earliest'].groupby(data['user']).rank(method='dense', ascending=False)
# print(get_feature())
# data = data[["user", "item", "label", "which"] + get_feature()]
train = data[(data.which >= 1)]
test = data[data.which == 0]
# test = test[(test.uif_3_4 == 0) | (test.uif_4_4 == 0)]
print(train.shape[0])
print(test.shape[0])
print(train.info())
print(test.info())
dtrain = xgb.DMatrix(train.drop(["user", "item", "label", "which"], axis=1), label=train.label)
dtest = xgb.DMatrix(test.drop(["user", "item", "label", "which"], axis=1), label=test.label)
param = {
    'booster': 'gbtree',
    'max_depth': '7',
    'eta': 0.01,
    'objective': 'binary:logistic',
    'eval_metric': 'auc',
    'subsample': 0.75,
    'colsample_bytree': 0.75,
    # 'gamma': 1,
    # 'lambda': 2,
    'nthread': 3,
}
watchlist = []
watchlist.append((dtrain, 'train'))
# watchlist.append((dtest, 'test'))
bst = xgb.train(param, dtrain, 400, watchlist)
preds = bst.predict(dtest)
result = pd.DataFrame(columns=["user", "item", "label", "predict"])
result.user = test.user
result.item = test.item
result.label = test.label
result.predict = preds
result.to_csv(path + "xgb_predict_online_2_" + select_days + "d.csv", index=False)

feature_score = bst.get_fscore()
feature_score = sorted(feature_score.items(), key=lambda x: x[1], reverse=True)
fs = []
for key, value in feature_score:
    fs.append("{0},{1}\n".format(key, value))
f = open(path + 'feature_score_2.csv', 'w')
f.writelines("feature,score\n")
f.writelines(fs)
