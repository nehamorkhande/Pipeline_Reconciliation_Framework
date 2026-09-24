# Databricks notebook source
import random
from datetime import datetime, timedelta
import pandas as pd

# COMMAND ----------

_seed = int(datetime.now().timestamp())
random.seed(_seed)
print(f"Random seed for this run: {_seed}")

# COMMAND ----------

dbutils.widgets.text("inject_issues", "true")
INJECT_ISSUES = dbutils.widgets.get("inject_issues").strip().lower() == "true"
print(f"INJECT_ISSUES = {INJECT_ISSUES}")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.hospital_recon_bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.hospital_recon_bronze.row_files")

# COMMAND ----------

departments = pd.DataFrame([
    {"dept_id": 1, "dept_name": "Cardiology", "building": "A"},
    {"dept_id": 2, "dept_name": "Orthopedics", "building": "B"},
    {"dept_id": 3, "dept_name": "Neurology", "building": "A"},
    {"dept_id": 4, "dept_name": "Pediatrics", "building": "C"},
    {"dept_id": 5, "dept_name": "Oncology", "building": "D"},
])


# COMMAND ----------

doctor_names = ["Dr. Sharma", "Dr. Mehta", "Dr. Rao", "Dr. Iyer", "Dr. Khan",
                 "Dr. Patil", "Dr. Gupta", "Dr. Nair", "Dr. Joshi", "Dr. Singh"]

# COMMAND ----------

doctors=pd.DataFrame([
    {
        "doctor_id": i + 1,
        "name": doctor_names[i],
        "specialization": random.choice(["Surgeon", "Physician", "Consultant"]),
        "dept_id": random.choice(departments["dept_id"].tolist())
    }
    for i in range(10)
])

# COMMAND ----------

first_names = ["Amit", "Priya", "Rahul", "Sneha", "Vikram", "Anita", "Suresh",
               "Kavita", "Rohan", "Neha", "Arjun", "Pooja", "Manoj", "Divya", "Karan", "Aarav", "Vivaan", "Aditya", "Krishna", "Ishaan", "Aryan", "Dhruv", "Yash","Nikhil", "Siddharth", "Akash", "Varun", "Harsh", "Ritesh", "Abhishek", "Aniket", "Sameer", "Mohit","Deepak", "Nitin", "Ajay", "Vijay", "Rakesh", "Mukesh", "Prakash", "Sunil", "Anil", "Mahesh", "Rajesh", "Pankaj", "Shubham", "Ayush", "Tushar", "Gaurav", "Naveen", "Tarun", "Hemant", "Sanjay", "Ravindra", "Dinesh", "Asha", "Aditi", "Akanksha", "Alka","Amrita", "Anjali", "Ankita", "Archana", "Arti", "Bhavna", "Chhavi", "Deepika", "Ekta", "Garima", "Geeta", "Harini", "Ishita", "Jaya", "Juhi", "Kanika", "Khushi", "Kirti", "Komal", "Lata", "Madhuri", "Meera", "Monika", "Namrata", "Nandini", "Nikita", "Pallavi", "Parul", "Payal", "Preeti", "Rachana", "Radhika", "Ritika", "Sakshi", "Saloni", "Shalini", "Shilpa", "Shraddha", "Shreya", "Smita", "Sonal", "Swati", "Tanvi", "Trisha", "Vaishali", "Vandana", "Vidya", "Yamini", "Zara", "Ananya", "Myra", "Siya", "Diya", "Avni"]

# COMMAND ----------

last_names = [
    "Sharma", "Verma", "Patel", "Reddy", "Kumar", "Singh", "Gupta", "Nair",
    "Iyer", "Menon", "Rao", "Joshi", "Patil", "Kulkarni", "Desai", "Mehta",
    "Jain", "Agarwal", "Agrawal", "Choudhary", "Chauhan", "Yadav", "Mishra",
    "Pandey", "Tiwari", "Tripathi", "Dubey", "Shukla", "Srivastava", "Sinha",
    "Thakur", "Rajput", "Saxena", "Bansal", "Mittal", "Goel", "Kapoor",
    "Malhotra", "Arora", "Bhatia", "Khanna", "Sethi", "Anand", "Bajaj",
    "Oberoi", "Gill", "Sandhu", "Kaur", "Grewal", "Brar", "Sidhu",
    "Chatterjee", "Banerjee", "Mukherjee", "Bhattacharya", "Bose", "Roy",
    "Das", "Sen", "Dutta", "Ghosh", "Pal", "Mondal", "Kar", "Sarkar",
    "Mohanty", "Panda", "Nayak", "Pradhan", "Swain", "Behera",
    "Krishnan", "Subramanian", "Narayanan", "Pillai", "Nambiar",
    "Ramaswamy", "Venkatesh", "Balakrishnan", "Sundaram", "Ramachandran",
    "Shetty", "Hegde", "Acharya", "Bhat", "Kamath", "Pai",
    "Fernandes", "D'Souza", "Pinto", "Rodrigues", "Pereira", "Lobo",
    "Thomas", "Mathew", "George", "Joseph", "Abraham", "Sebastian",
    "Khan", "Ali", "Shaikh", "Ansari", "Qureshi", "Siddiqui", "Pathan",
    "Naik", "Sawant", "Jadhav", "Shinde", "Pawar", "Mane", "More",
    "Gaikwad", "Chavan", "Salunkhe", "Wagh", "Kale", "Bhosale",
    "Rawat", "Negi", "Bisht", "Pant", "Joshi", "Bhardwaj",
    "Tyagi", "Rastogi", "Mathur", "Mahajan", "Lal", "Chopra",
    "Bedi", "Puri", "Sachdeva", "Ahuja", "Tandon", "Narang"
]

# COMMAND ----------

cities=["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad", "Thane",
    "Navi Mumbai", "Kalyan", "Dombivli", "Mira Bhayandar",
    "Vasai", "Virar", "Ulhasnagar", "Amravati", "Akola",
    "Yavatmal", "Chandrapur", "Wardha", "Bhandara", "Gondia",
    "Gadchiroli", "Washim", "Buldhana", "Jalgaon", "Dhule",
    "Nandurbar", "Ahmednagar", "Shirdi", "Kopargaon", "Sangamner",
    "Solapur", "Barshi", "Pandharpur", "Sangli", "Miraj",
    "Kolhapur", "Ichalkaranji", "Satara", "Karad", "Mahabaleshwar",
    "Ratnagiri", "Chiplun", "Sindhudurg", "Malvan", "Kankavli",
    "Latur", "Osmanabad", "Beed", "Parbhani", "Hingoli",
    "Nanded", "Jalna", "Palghar", "Alibag", "Panvel",
    "Karjat", "Khopoli", "Lonavala", "Khandala", "Matheran",
    "Bhiwandi", "Malegaon", "Bhusawal", "Amalner", "Jalna",
    "Udgir", "Nilanga", "Tuljapur", "Akluj", "Indapur",
    "Shirur", "Baramati", "Daund", "Saswad", "Thergaon",
    "Pimpri", "Chinchwad", "Talegaon", "Dehu", "Alandi"]

# COMMAND ----------

blood_groups = ["A+", "B+", "AB+", "O+", "A-", "B-", "O-"]

# COMMAND ----------

patients = pd.DataFrame([
    {
        "patient_id": i + 1,
        "name": f"{random.choice(first_names)} {random.choice(last_names)}",
        "age": random.randint(1, 90),
        "gender": random.choice(["M", "F"]),
        "blood_group": random.choice(blood_groups),
        "city": random.choice(cities)
    }
    for i in range(5000)
])

# COMMAND ----------

if INJECT_ISSUES:
    n_dup_patients = random.randint(8, 25)
    dup_patients = patients.sample(n=n_dup_patients).copy()
    dup_patients["patient_id"] = range(5001, 5001 + len(dup_patients))
    patients = pd.concat([patients, dup_patients], ignore_index=True)
    print(f"Injected {len(dup_patients)} duplicate patient records")
 

# COMMAND ----------

diagnosis_codes = ["A001", "B002", "C003", "D004", "E005", "F006", "G007"]

# COMMAND ----------

TOTAL_ENCOUNTERS = 100000
CHUNK_SIZE = 20000

# COMMAND ----------

start_date = datetime(2025, 1, 1)
base_path = "/Volumes/workspace/hospital_recon_bronze/raw_files"

# COMMAND ----------

departments.to_csv(f"{base_path}/departments.csv", index=False)
doctors.to_csv(f"{base_path}/doctors.csv", index=False)
patients.to_csv(f"{base_path}/patients.csv", index=False)
 
print("Generating 1,00,000 encounter records in chunks...")

# COMMAND ----------

all_chunks = []
for chunk_start in range(0, TOTAL_ENCOUNTERS, CHUNK_SIZE):
    chunk_end = min(chunk_start + CHUNK_SIZE, TOTAL_ENCOUNTERS)
    encounters_chunk = []
    for i in range(chunk_start, chunk_end):
        enc_date = start_date + timedelta(days=random.randint(0, 500))
        encounters_chunk.append({
            "encounter_id": i + 1,
            "patient_id": random.randint(1, 5000),
            "doctor_id": random.randint(1, 10),
            "dept_id": random.randint(1, 5),
            "encounter_date": enc_date.strftime("%Y-%m-%d"),
            "billing_amount": round(random.uniform(500, 50000), 2),
            "length_of_stay": random.randint(0, 15),
            "diagnosis_code": random.choice(diagnosis_codes)
        })
 
    all_chunks.append(pd.DataFrame(encounters_chunk))
    print(f"  Generated rows {chunk_start + 1} to {chunk_end}")

# COMMAND ----------

encounters = pd.concat(all_chunks, ignore_index=True)


# COMMAND ----------


if INJECT_ISSUES:
    print("\nInjecting realistic data quality issues...")
 
    # Counts vary per run within realistic ranges, so different runs produce
    # different total mismatch numbers (not identical every time).
    n_dups = random.randint(20, 80)
    n_null_billing = random.randint(15, 45)
    n_neg_billing = random.randint(5, 20)
    n_orphans = random.randint(10, 35)
 
    # 1. Duplicate rows - same encounter appears twice (re-ingestion glitch)
    dup_rows = encounters.sample(n=n_dups).copy()
    encounters = pd.concat([encounters, dup_rows], ignore_index=True)
    print(f"  Added {len(dup_rows)} duplicate rows")
 
    # 2. Null billing_amount - failed billing entries
    null_idx = encounters.sample(n=n_null_billing).index
    encounters.loc[null_idx, "billing_amount"] = None
    print(f"  Set {len(null_idx)} rows to NULL billing_amount")
 
    # 3. Negative billing_amount - refund/data entry errors
    neg_idx = encounters.sample(n=n_neg_billing).index
    encounters.loc[neg_idx, "billing_amount"] = -abs(encounters.loc[neg_idx, "billing_amount"])
    print(f"  Set {len(neg_idx)} rows to NEGATIVE billing_amount")
 
    # 4. Orphan patient_id - patient_id not present in patients.csv
    orphan_idx = encounters.sample(n=n_orphans).index
    encounters.loc[orphan_idx, "patient_id"] = encounters["patient_id"].max() + pd.Series(
        range(1, len(orphan_idx) + 1), index=orphan_idx
    )
    print(f"  Set {len(orphan_idx)} rows to ORPHAN patient_id (not in dim_patient)")
 
    print(f"\n  Total raw encounter rows (with issues): {len(encounters)}")
else:
    print("\nINJECT_ISSUES=False - generating clean data (no data quality issues).")
    print(f"  Total raw encounter rows: {len(encounters)}")
 

# COMMAND ----------

encounters.to_csv(f"{base_path}/encounters.csv", index=False)
print(f"  Written {len(encounters)} rows to {base_path}/encounters.csv")
 

# COMMAND ----------

print("\nGenerated files:")
for f in ["departments.csv", "doctors.csv", "patients.csv", "encounters.csv"]:
    print(f"  {base_path}/{f}")

# COMMAND ----------

print("\nRow counts:")
print(f"  departments: {len(departments)}")
print(f"  doctors:     {len(doctors)}")
if INJECT_ISSUES:
    print(f"  patients:    {len(patients)}  (includes {n_dup_patients} duplicates)")
    print(f"  encounters:  {len(encounters)}  (includes {n_dups} dup, {n_null_billing} null billing, "
          f"{n_neg_billing} negative billing, {n_orphans} orphan patient_id)")
    print("\nThese issues will be cleaned/filtered in Silver layer,")
    print("causing Bronze != Gold counts -> reconciliation FAIL (expected & realistic).")
else:
    print(f"  patients:    {len(patients)}")
    print(f"  encounters:  {len(encounters)}")
    print("\nClean data - Bronze == Gold expected -> reconciliation PASS.")