from lottery import Bet

_ENCODING = "utf-8"

def to_bet(agency_id: int, csv: bytes) -> Bet:
    fields = csv.decode(_ENCODING).strip().split(",")
    
    return Bet(
        agency_id=agency_id,
        first_name=fields[0],
        last_name=fields[1],
        document=int(fields[2]),
        birthdate=fields[3],
        number=int(fields[4])
    )

def to_csv(bet: Bet) -> str:
    return f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
