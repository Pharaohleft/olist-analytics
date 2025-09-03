# -*- coding: utf-8 -*-
import os
import argparse
import numpy as np
import pandas as pd

def parse_floats(csv_str, name):
    try:
        return [float(x) for x in csv_str.split(",") if x.strip() != ""]
    except Exception as e:
        raise ValueError(f"Bad list for {name}: {csv_str}") from e

def load_preds(preds_path):
    if not os.path.exists(preds_path):
        raise FileNotFoundError(f"Missing {preds_path}. Run the snapshot trainer first.")
    df = pd.read_csv(preds_path)
    need = {"cust_uid", "churn_prob"}
    if not need.issubset(df.columns):
        raise ValueError(f"{preds_path} must contain columns: {need}")
    df = df.sort_values(["cust_uid", "churn_prob"], ascending=[True, False]).drop_duplicates("cust_uid", keep="first")
    return df[["cust_uid", "churn_prob"]].reset_index(drop=True)

def load_aov(snapshot_path):
    if os.path.exists(snapshot_path):
        snap = pd.read_csv(snapshot_path, usecols=["cust_uid", "avg_order_value"])
        snap["avg_order_value"] = snap["avg_order_value"].fillna(0)
        return snap
    return pd.DataFrame(columns=["cust_uid", "avg_order_value"])

def simulate(df, targets, discounts, margin, beta, n_mc, seed):
    """
    Assumptions:
      p0  = baseline return prob ~= (1 - churn_prob)
      Treat with discount d:
        p1 = clip(p0 + beta * d * (1 - p0), 0, 1)
      Profit per purchase:
        baseline: aov * margin
        treated : aov * (margin - d)
      We compare expected treated vs baseline profit on the targeted set.
      CIs via Monte Carlo over Bernoulli draws.
    """
    rng = np.random.default_rng(seed)
    out = []

    df = df.copy()
    df["p0"] = (1 - df["churn_prob"]).clip(1e-6, 1 - 1e-6)
    df["avg_order_value"] = df.get("avg_order_value", pd.Series(0, index=df.index)).fillna(0)

    for tp in targets:
        n = len(df)
        k = int(np.ceil(tp * n))
        targeted = df.sort_values("churn_prob", ascending=False).head(k).copy()

        p0 = targeted["p0"].to_numpy()
        aov = targeted["avg_order_value"].to_numpy()
        if np.all((aov == 0) | np.isnan(aov)):
            aov = np.full_like(p0, 100.0, dtype=float)  # global fallback

        for d in discounts:
            p1 = np.clip(p0 + beta * d * (1 - p0), 0, 1)

            base_profit_exp  = float((p0 * aov * margin).sum())
            treat_profit_exp = float((p1 * aov * (margin - d)).sum())
            delta_exp = treat_profit_exp - base_profit_exp

            exp_disc_cost = float((p1 * aov * d).sum())
            roi = (delta_exp / exp_disc_cost) if exp_disc_cost > 0 else float("nan")

            lift = float(np.mean(p1 - p0))

            deltas = []
            for _ in range(n_mc):
                b0 = rng.binomial(1, p0)
                b1 = rng.binomial(1, p1)
                delta = (b1 * aov * (margin - d) - b0 * aov * margin).sum()
                deltas.append(delta)
            deltas = np.array(deltas)
            ci_lo, ci_hi = np.percentile(deltas, [2.5, 97.5])

            out.append({
                "target_pct": tp,
                "discount": d,
                "n_target": k,
                "avg_aov": float(np.nanmean(aov)),
                "avg_p0": float(p0.mean()),
                "avg_p1": float(p1.mean()),
                "lift_abs": lift,
                "delta_profit_expectation": float(delta_exp),
                "delta_profit_mc_mean": float(deltas.mean()),
                "delta_profit_ci_lo": float(ci_lo),
                "delta_profit_ci_hi": float(ci_hi),
                "treat_roi": float(roi),
            })

    res = pd.DataFrame(out).sort_values(["delta_profit_expectation"], ascending=False)
    return res

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preds", default=os.path.join("outputs", "churn_predictions_snapshot.csv"))
    ap.add_argument("--snapshot", default=os.path.join("outputs", "churn_snapshot.csv"))
    ap.add_argument("--targets", default="0.1,0.2,0.25,0.3")
    ap.add_argument("--discounts", default="0.05,0.07,0.10")
    ap.add_argument("--margin", type=float, default=0.25)
    ap.add_argument("--beta", type=float, default=0.50)
    ap.add_argument("--n-mc", type=int, default=300)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join("outputs", "ab_sim_results.csv"))
    args = ap.parse_args()

    targets = parse_floats(args.targets, "targets")
    discounts = parse_floats(args.discounts, "discounts")

    preds = load_preds(args.preds)
    snap  = load_aov(args.snapshot)

    df = preds.merge(snap, on="cust_uid", how="left")
    res = simulate(df, targets, discounts, args.margin, args.beta, args.n_mc, args.seed)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    res.to_csv(args.out, index=False)

    print("\\nTop policies by expected delta profit:")
    print(res.head(5).to_string(index=False))
    print(f"\\nWrote {args.out} ({len(res)} rows)")

if __name__ == "__main__":
    main()
