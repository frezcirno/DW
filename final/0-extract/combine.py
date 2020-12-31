import os

count = os.cpu_count()

with open("final.jl", "w") as out, open("final.log", "w") as log, open(
    "final.err", "w"
) as err:
    for i in range(count):
        with open(f"{i}.jl") as iout, open(f"{i}.log") as ilog, open(
            f"{i}.err"
        ) as ierr:
            out.write(iout.read())
            log.write(ilog.read())
            err.write(ierr.read())

        os.remove(f"{i}.jl")
        os.remove(f"{i}.log")
        os.remove(f"{i}.err")
