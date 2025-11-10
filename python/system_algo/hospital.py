# Hospital Patient & Bed Management Analytics

# You’re building a real-time analytics system for hospitals to track patients, beds, and doctors across multiple hospitals
# in a city or region. The system should support both active/current stats and aggregates like total treatment durations.

# APIs to implement:

# start_admission(patient_id, name, hospital, doctor_id, bed_id, admission_time)
# # Marks patient as admitted and occupies a bed

# end_admission(patient_id, discharge_time)
# # Marks patient discharge, releases the bed, updates doctor stats and hospital stats

# transfer_patient(patient_id, new_hospital, new_bed_id)
# # Moves patient to a different hospital/bed

# get_current_occupied_beds(hospital: str | None) -> int
# # Number of beds currently occupied (global or per hospital)

# get_doctor_stats(doctor_id: int) -> tuple[num_patients, total_treatment_duration]

# get_hospital_summary(hospital: str) -> tuple[num_patients, num_active_patients, avg_treatment_time]

# get_top_doctors_by_patients(k: int) -> list[doctor_id]

# get_top_hospitals_by_patient_turnover(k: int) -> list[hospital]

from collections import defaultdict
from dataclasses import dataclass
import heapq

from heapdict import heapdict

@dataclass
class Patient:
    id: str
    name: str
    hospital: str
    doctor_id: str
    bed_id: str
    admission_time: int

class HospitalSystem:
    def __init__(self,
        top_k_default:int = 100,
    ) -> None:
        self.top_k_default = top_k_default
        self.patients = {} # patient_id -> Patient

        self.hospital_occupied_beds = {} # hospital -> set(bed id)
        self.total_occupied_beds = 0

        self.top_k_doctors = heapdict()
        self.top_k_hospitals = heapdict()
        self.hospital_stats = defaultdict(lambda: {
            "num_discharged": 0,
            "num_active_patients": 0,
            "total_treatment_time": 0,
        })
        self.doctor_stats = defaultdict(lambda: {
            "num_treated_patients": 0,
            "total_treatment_duration": 0,
        })

    def _update_top_k_doctors(self, doctor_id: str, num_treated: int):
        if (
            len(self.top_k_doctors) < self.top_k_default or
            doctor_id in self.top_k_doctors
        ):
            self.top_k_doctors[doctor_id] = num_treated
        else:
            _, lowest_treated = self.top_k_doctors.peekitem()
            if num_treated > lowest_treated:
                _ = self.top_k_doctors.popitem()
                self.top_k_doctors[doctor_id] = num_treated

    def _update_top_k_hospitals(self, hospital: str, num_discharged: int):
        if (
            len(self.top_k_hospitals) < self.top_k_default or
            hospital in self.top_k_hospitals
        ):
            self.top_k_hospitals[hospital] = num_discharged
        else:
            _, lowest_discharges = self.top_k_hospitals.peekitem()
            if num_discharged > lowest_discharges:
                _ = self.top_k_hospitals.popitem()
                self.top_k_hospitals[hospital] = num_discharged

    def start_admission(self, patient_id: str, name: str, hospital: str, doctor_id: str, bed_id: str, admission_time: int):
        # Marks patient as admitted and occupies a bed

        if patient_id in self.patients:
            return
        hospital_occupied_beds = self.hospital_occupied_beds.setdefault(hospital, set())
        if bed_id in hospital_occupied_beds:
            return

        patient = Patient(
            patient_id, name, hospital, doctor_id, bed_id, admission_time
        )
        self.patients[patient_id] = patient

        hospital_occupied_beds.add(bed_id)
        self.total_occupied_beds += 1
        hospital_stats = self.hospital_stats[hospital]
        hospital_stats["num_active_patients"] += 1

    def end_admission(self, patient_id, discharge_time):
        # Marks patient discharge, releases the bed, updates doctor stats and hospital stats
        if patient_id not in self.patients:
            return

        patient = self.patients[patient_id]
        if discharge_time < patient.admission_time:
            return

        del self.patients[patient_id]
        hospital_occupied_beds = self.hospital_occupied_beds.setdefault(patient.hospital, set())
        hospital_occupied_beds.discard(patient.bed_id)
        self.total_occupied_beds -= 1

        treatment_time = discharge_time - patient.admission_time
        hospital_stats = self.hospital_stats[patient.hospital]
        hospital_stats["num_discharged"] += 1
        hospital_stats["num_active_patients"] -= 1
        hospital_stats["total_treatment_time"] += treatment_time

        doctor_stats = self.doctor_stats[patient.doctor_id]
        doctor_stats["num_treated_patients"] += 1
        doctor_stats["total_treatment_duration"] += treatment_time
        self._update_top_k_doctors(patient.doctor_id, doctor_stats["num_treated_patients"])
        self._update_top_k_hospitals(patient.hospital, hospital_stats["num_discharged"])

    def transfer_patient(self, patient_id, new_hospital, new_bed_id):
        # Moves patient to a different hospital/bed
        if patient_id not in self.patients:
            return # TODO should return error here

        new_hospital_occupied_beds = self.hospital_occupied_beds.setdefault(new_hospital, set())
        if new_bed_id in new_hospital_occupied_beds:
            return # TODO should return error here

        patient = self.patients[patient_id]
        hospital_occupied_beds = self.hospital_occupied_beds.setdefault(patient.hospital, set())
        hospital_occupied_beds.discard(patient.bed_id)
        hospital_stats = self.hospital_stats[patient.hospital]
        hospital_stats["num_active_patients"] -= 1

        patient.hospital = new_hospital
        patient.bed_id = new_bed_id
        new_hospital_occupied_beds.add(new_bed_id)
        hospital_stats = self.hospital_stats[new_hospital]
        hospital_stats["num_active_patients"] += 1

    
    def get_current_occupied_beds(self, hospital: str | None) -> int:
        # Number of beds currently occupied (global or per hospital)
        if hospital:
            if hospital not in self.hospital_occupied_beds:
                return 0

            return len(self.hospital_occupied_beds[hospital])
        return self.total_occupied_beds

    def get_doctor_stats(self, doctor_id: int) -> tuple[int, int]:
        if doctor_id not in self.doctor_stats:
            return (0, 0)

        return (
            self.doctor_stats[doctor_id]["num_treated_patients"],
            self.doctor_stats[doctor_id]["total_treatment_duration"],
        )
    
    def get_hospital_summary(self, hospital: str) -> tuple[int, int, float]:
        if hospital not in self.hospital_stats:
            return (0, 0, 0.)

        hospital_stats = self.hospital_stats[hospital]
        avg_treatment_time = 0.
        if hospital_stats["num_discharged"] > 0:
            avg_treatment_time = hospital_stats["total_treatment_time"] / hospital_stats["num_discharged"]
        return (
            hospital_stats["num_discharged"],
            hospital_stats["num_active_patients"],
            round(avg_treatment_time, 3),
        )
        
    def get_top_doctors_by_patients(self, k: int) -> list[str]:
        # top k by number of treated patients
        k = min(k, self.top_k_default)
        top_k_doctors = dict(self.top_k_doctors)
        return heapq.nlargest(
            k,
            self.top_k_doctors.items(),
            key=lambda doctor_id_num_treated: doctor_id_num_treated[1],
        )
        
    def get_top_hospitals_by_patient_turnover(self, k: int) -> list[str]:
        # top k hospitals by number of discharges
        k = min(k, self.top_k_default)
        top_k_hospital = dict(self.top_k_hospitals)
        return heapq.nlargest(
            k,
            self.top_k_hospitals.items(),
            key=lambda hospital_num_discharged: hospital_num_discharged[1],
        )
