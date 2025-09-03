import os
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix

INPATH = os.path.join('outputs', 'churn_features.csv')
OUT_PRED = os.path.join('outputs', 'churn_predictions.csv')

df = pd.read_csv(INPATH)

# --- Label ---
# Heuristic: churn if no purchase within last 90 days at ref date
df['churn'] = (df['recency_days'] >= 90).astype(int)

# --- Features ---
feature_cols = [
    'recency_days','frequency','monetary','r','f','m',
    'orders_30d','orders_60d','orders_90d',
    'avg_order_value','avg_delivery_days','avg_review_score',
    'pay_share_card','pay_share_boleto','pay_share_voucher',
    'heavy_bulky_share','ontime_rate'
]

X = df[feature_cols].copy()
y = df['churn'].copy()

# Clean NaNs / infs
X = X.replace([np.inf, -np.inf], np.nan).fillna(0)

# Train/test split (stratified)
X_train, X_test, y_train, y_test, cust_train, cust_test = train_test_split(
    X, y, df['cust_uid'], test_size=0.3, random_state=42, stratify=y
)

# Pipeline: scale + logistic regression (balanced)
pipe = Pipeline(steps=[
    ('scaler', StandardScaler(with_mean=True, with_std=True)),
    ('logit', LogisticRegression(max_iter=1000, class_weight='balanced', solver='lbfgs'))
])

pipe.fit(X_train, y_train)

# Evaluate
proba = pipe.predict_proba(X_test)[:,1]
pred = (proba >= 0.5).astype(int)

auc = roc_auc_score(y_test, proba)
print(f'ROC-AUC: {auc:.4f}\\n')

print('Classification report (thr=0.5):')
print(classification_report(y_test, pred, digits=3))

print('Confusion matrix:')
print(confusion_matrix(y_test, pred))

# Save predictions
out = pd.DataFrame({
    'cust_uid': cust_test,
    'churn_prob': proba,
    'churn_pred_0_1': pred,
    'churn_true': y_test.values
}).sort_values('churn_prob', ascending=False)

out.to_csv(OUT_PRED, index=False)
print(f'Wrote {OUT_PRED} ({len(out):,} rows)')
