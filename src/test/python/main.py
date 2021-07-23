import random
from os import popen
from shutil import rmtree


def main():
    lower, upper = 0, 10
    with open("ranges.txt", "w") as f:
        f.write(f"{lower},{upper}\n")
        f.write(f"{lower},{upper}\n")
    with open("input/points.txt", "w") as f:
        for _ in range(100_000):
            x = random.random() * (upper - lower) + lower
            y = random.random() * (upper - lower) + lower
            f.write(f"{x:.2f},{y:.2f}\n")

    rmtree("output")
    stream = popen('hadoop jar k-mean.jar input ranges.txt 3')
    output = stream.read()
    print(output)


if __name__ == '__main__':
    main()
