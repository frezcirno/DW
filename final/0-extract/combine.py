import os

count = os.cpu_count()

with open("extract_logs/final.jl", "w") as out, open("extract_logs/final.log", "w") as log, open(
    "extract_logs/final.err", "w"
) as err:
    for i in range(count):
        with open(f"extract_logs/{i}.jl") as iout, open(f"extract_logs/{i}.log") as ilog, open(
            f"extract_logs/{i}.err"
        ) as ierr:
            out.write(iout.read())
            log.write(ilog.read())
            err.write(ierr.read())

        os.remove(f"extract_logs/{i}.jl")
        os.remove(f"extract_logs/{i}.log")
        os.remove(f"extract_logs/{i}.err")
