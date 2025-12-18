import os
import time
import random
from datetime import datetime

# 1. Configuration - Set your landing zone path
# If running in Databricks, use /dbfs/mnt/... or a local temp path
LANDING_ZONE_PATH = "/dbfs/mnt/healthcare/landing/hl7_adt/"
os.makedirs(LANDING_ZONE_PATH, exist_ok=True)

def generate_hl7_adt(patient_id, event_type="A01"):
    """Generates a synthetic HL7 ADT message string."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    msg_id = random.randint(10000, 99999)
    
    # MSH Segment
    msh = f"MSH|^~\\&|MOCK_EMR|HOSPITAL|DATABRICKS|CA|{timestamp}||ADT^{event_type}|{msg_id}|P|2.5"
    
    # PID Segment
    names = ["DOE^JOHN", "SMITH^JANE", "BROWN^BOB", "WILLIAMS^ALICE"]
    pid = f"PID|1||{patient_id}^^^MRN||{random.choice(names)}^^^^||19850101|M|||123 DATA ST^^SF^CA^94105"
    
    # PV1 Segment
    units = ["ICU", "MEDSURG", "ER", "CARDIAC"]
    pv1 = f"PV1|1|I|{random.choice(units)}^BED{random.randint(1,20)}^01||||1234^STRANGE^STEPHEN||||||||ADM|A0"
    
    return f"{msh}\r{pid}\r{pv1}"

def stream_mock_data(count=10, interval_seconds=5):
    """Simulates a live hospital feed by dropping files over time."""
    print(f"Starting mock feed to {LANDING_ZONE_PATH}...")
    
    for i in range(count):
        patient_id = f"PAT{1000 + i}"
        
        # 1. Generate Admission (A01)
        adt_msg = generate_hl7_adt(patient_id, "A01")
        file_name = f"adt_{patient_id}_{int(time.time())}.hl7"
        
        with open(os.path.join(LANDING_ZONE_PATH, file_name), "w") as f:
            f.write(adt_msg)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Dropped Admission: {file_name}")
        
        # Wait before the next message
        time.sleep(interval_seconds)

# Run the generator
stream_mock_data(count=20, interval_seconds=10)
