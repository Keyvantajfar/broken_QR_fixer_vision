# import lca_pipeline as lca
import LCA_GPT as lca

print("[DEBUG] Starting test_LCA script")
results = lca.run_lca('20250408-dsRNA in vitro synthesis-LCA calculation.xlsx', ui_list=['U_1'], write_back=True)
for name, df in results.items():
    print(f"[DEBUG] Result {name} with shape {df.shape}")
print("[DEBUG] test_LCA completed")
