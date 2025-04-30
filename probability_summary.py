# Code 6: Summarizes the probability of savings lasting 30 years for initial savings of $500,000 to $1,000,000 with $40,000 fixed annual expenses (Table 1).

### PySpark Implementation
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, lit, array, sum as spark_sum
from pyspark.sql.types import DoubleType, ArrayType
import numpy as np
 
# Initialize Spark session
spark = SparkSes-si-on.builder.appName("RetirementSimulation").getOrCreate()
 
# Parameters
initial_savings_range = list(range(500000, 1000001, 100000))  # 500k to 1M in 100k increments
annual_expense = 40000  # Fixed $40,000 per year
expected_return = 0.06
volatility = 0.15
years = 30
num_simulations = 10000
 
# Monte Carlo Simulation: Check if savings last 30 years and return final savings
def monte_carlo_simulation(initial_savings):
    savings = initial_savings
    annual_returns = np.random.normal(expected_return, volatility, years)
    final_savings_list = []
    
    for year in range(years):
        savings = savings * (1 + annual_returns[year]) - annual_expense
        if savings <= 0:
            final_savings_list.append(0.0)
            return final_savings_list, 0  # Savings de-pleted
    final_savings_list.append(max(savings, 0))
    return final_savings_list, 1  # Savings lasted
 
# Stress Test: -30% return in year 1
def stress_test_simulation(initial_savings, stress_return=-0.3):
    savings = initial_savings
    annual_returns = [stress_return] + list(np.random.normal(expected_return, volatility, years-1))
    final_savings_list = []
    
    for year in range(years):
        savings = savings * (1 + annual_returns[year]) - annual_expense
        if savings <= 0:
            final_savings_list.append(0.0)
            return final_savings_list, 0
    final_savings_list.append(max(savings, 0))
    return final_savings_list, 1
 
# Hybrid Test: 2% fixed return for 5 years, then Monte Carlo
def hybrid_simulation(initial_savings):
    savings = initial_savings
    annual_returns = [0.02] * 5 + list(np.random.normal(expected_return, volatility, years-5))
    final_savings_list = []
    
    for year in range(years):
        savings = savings * (1 + annual_returns[year]) - annual_expense
        if savings <= 0:
            final_savings_list.append(0.0)
            return final_savings_list, 0
    final_savings_list.append(max(savings, 0))
    return final_savings_list, 1
 
# Register UDFs
monte_carlo_udf = udf(lambda x: mon-te_carlo_simulation(x), ArrayType(DoubleType(), con-tainsNull=False), DoubleType())
stress_test_udf = udf(lambda x: stress_test_simulation(x), ArrayType(DoubleType(), con-tainsNull=False), DoubleType())
hybrid_udf = udf(lambda x: hybrid_simulation(x), Array-Type(DoubleType(), containsNull=False), DoubleType())
 
# Create DataFrame with initial savings
data = [(float(s),) for s in initial_savings_range]
df = spark.createDataFrame(data, ["initial_savings"])
 
# Run simulations
df_mc = df.withColumn("mc_results", mon-te_carlo_udf(col("initial_savings")))
df_mc = df_mc.withColumn("mc_final_savings", col("mc_results")[0])
df_mc = df_mc.withColumn("mc_success", col("mc_results")[1])
df_mc = df_mc.groupBy("initial_savings").agg(
    (spark_sum(col("mc_success")) / num_simulations * 100).alias("mc_success_rate"),
    {"mc_final_savings": "ap-prox_percentile(mc_final_savings, 0.05)"}
).alias("mc_var")
 
df_stress = df.withColumn("stress_results", stress_test_udf(col("initial_savings")))
df_stress = df_stress.withColumn("stress_success", col("stress_results")[1])
df_stress = df_stress.groupBy("initial_savings").agg(
    (spark_sum(col("stress_success")) / num_simulations * 100).alias("stress_success_rate")
)
 
df_hybrid = df.withColumn("hybrid_results", hy-brid_udf(col("initial_savings")))
df_hybrid = df_hybrid.withColumn("hybrid_success", col("hybrid_results")[1])
df_hybrid = df_hybrid.groupBy("initial_savings").agg(
    (spark_sum(col("hybrid_success")) / num_simulations * 100).alias("hybrid_success_rate")
)
 
# Collect results
mc_results = df_mc.collect()
stress_results = df_stress.collect()
hybrid_results = df_hybrid.collect()
 
# Format table
print(f"{'INITIAL SAVINGS':<15} {'ANNUAL EXPENSES':<15} {'% OF SCENARIOS SAVINGS LAST 30 YEARS (MONTE CARLO)':<45} {'% (STRESS TEST)':<15} {'% (HYBRID TEST)':<15}")
for i in range(len(mc_results)):
    initial = mc_results[i]["initial_savings"]
    mc_success = mc_results[i]["mc_success_rate"]
    stress_success = stress_results[i]["stress_success_rate"]
    hybrid_success = hy-brid_results[i]["hybrid_success_rate"]
    print(f"${initial:,.0f}{'' :<5} ${annu-al_expense:,.0f}{'' :<8} {mc_success:.1f}%{'' :<35} {stress_success:.1f}%{'' :<5} {hybrid_success:.1f}%")
 
# Economic Capital and VaR (calculated separately)
print("\nAdditional Metrics:")
print(f"{'INITIAL SAVINGS':<15} {'5% VaR (FINAL SAVINGS)':<25} {'ECONOMIC CAPITAL (99.5% CONFIDENCE)':<35}")
for row in mc_results:
    initial = row["initial_savings"]
    var = row["approx_percentile(mc_final_savings, 0.05)"]
    # Economic Capital: Increase initial savings until 99.5% success rate
    target_success = 0.995
    test_savings = initial
    step = 10000
    success_rate = 0.0
    while success_rate < target_success and test_savings < 5000000:
        test_results = [mon-te_carlo_simulation(test_savings)[1] for _ in range(num_simulations)]
        success_rate = sum(test_results) / num_simulations
        if success_rate >= target_success:
            break
        test_savings += step
    econ_capital = test_savings - initial
    print(f"${initial:,.0f}{'' :<5} ${var:,.0f}{'' :<15} ${econ_capital:,.0f}")
 
# Stop Spark session
spark.stop()
