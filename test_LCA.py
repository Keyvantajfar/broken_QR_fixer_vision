# import lca_pipeline as lca
import LCA_GPT as lca

print("Running LCA test...")
results = lca.run_lca('20250408-dsRNA in vitro synthesis-LCA calculation.xlsx', write_back=True)
for name, df in results.items():
    print(f"Result {name} shape: {df.shape}")
