from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# ------------------------------------------------------------
# Project paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
input_file = project_root / "outputs" / "tables" / "top_genre_trends.csv"
output_dir = project_root / "outputs" / "figures"
output_file = output_dir / "genre_trends.png"

output_dir.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
df = pd.read_csv(input_file)

# Sort by best increase
df = df.sort_values(by="bestIncrease", ascending=False)

# ------------------------------------------------------------
# Plot
# ------------------------------------------------------------
plt.figure(figsize=(8, 5))
plt.bar(df["genre"], df["bestIncrease"])

plt.xlabel("Genre")
plt.ylabel("Rating Increase")
plt.title("Top Genre Rating Increases (Rolling Decade Analysis)")
plt.xticks(rotation=0)
plt.tight_layout()

# Save and show
plt.savefig(output_file, dpi=300, bbox_inches="tight")
plt.show()

print(f"Loaded data from: {input_file}")
print(f"Saved plot to: {output_file}")