from typing import Literal

# Assignment types as a narrow type for clarity and safety
AssignmentType = Literal["direct_mapping", "pseudorandom"]

# Sex assignment preference for pseudorandom experiments
SexAssignmentPreference = Literal["evenly_split", "male_only", "female_only"]
