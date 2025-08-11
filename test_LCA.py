# import lca_pipeline as lca
import LCA_GPT as lca
results = lca.run_lca('20250408-dsRNA in vitro synthesis-LCA calculation.xlsx', write_back=True)
# results is a dict: {'U_1_Midpoint': df, 'U_2_Midpoint': df, ...}
