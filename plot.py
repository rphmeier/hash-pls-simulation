import os
import csv
import matplotlib.pyplot as plt

def parse_filename(filename: str):
    filename = filename.split('.')[0]
    parts = filename.split("_")

    return (parts[0], parts[1])

def plot(filename, test, mapkind):
    if test == "churn":
        plot_churn(filename, mapkind)
    elif test == "grow":
        plot_grow(filename, mapkind)
    elif test == "probe":
        plot_probe(filename, mapkind)
    elif test == "overwrite":
        plot_overwrite(filename, mapkind)

def read_csv(filename):
    load_factors = []
    a_mean = []
    a_50 = []
    a_95 = []
    a_99 = []
    b_mean = []
    b_50 = []
    b_95 = []
    b_99 = []

    with open("out/" + filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            load_factors.append(float(row[0]))
            a_mean.append(float(row[1]))
            a_50.append(float(row[2]))
            a_95.append(float(row[3]))
            a_99.append(float(row[4]))

            if len(row) > 5:
                b_mean.append(float(row[5]))
                b_50.append(float(row[6]))
                b_95.append(float(row[7]))
                b_99.append(float(row[8]))

    return (load_factors, a_mean, a_50, a_95, a_99, b_mean, b_50, b_95, b_99)

def make_plot(plot_filename, csv_data, a_name, b_name):
    fig, ax = plt.subplots()
    ax.set(xlabel="load factor", ylabel="operations")
    ax.set_yscale('log')

    (load_factors, a_mean, a_50, a_95, a_99, b_mean, b_50, b_95, b_99) = csv_data
    plt.plot(load_factors, a_mean, label=f"{a_name} mean", color="blue")
    plt.plot(load_factors, a_50, label=f"{a_name} 50pct", color="blue", linestyle="dashdot", linewidth="0.5")
    plt.plot(load_factors, a_95, label=f"{a_name} 95pct", color="blue", linestyle="dashed", linewidth="0.5")
    plt.plot(load_factors, a_99, label=f"{a_name} 99pct", color="blue", linestyle="dotted", linewidth="0.5")

    if b_name != "skip":
        plt.plot(load_factors, b_mean, label=f"{b_name} mean", color="red")
        plt.plot(load_factors, b_50, label=f"{b_name} 50pct", color="red", linestyle="dashdot", linewidth="0.5")
        plt.plot(load_factors, b_95, label=f"{b_name} 95pct", color="red", linestyle="dashed", linewidth="0.5")
        plt.plot(load_factors, b_99, label=f"{b_name} 99pct", color="red", linestyle="dotted", linewidth="0.5")
        
    plt.legend()
    plt.savefig(plot_filename)


def plot_grow(filename, mapkind):
    data = read_csv(filename)
    plot_filename = "plot/" + "grow_" + mapkind
    make_plot(plot_filename, data, "probes", "writes")

def plot_churn(filename, mapkind):
    data = read_csv(filename)
    plot_filename = "plot/" + "churn_" + mapkind
    make_plot(plot_filename, data, "probes", "writes")


def plot_probe(filename, mapkind):
    data = read_csv(filename)
    plot_filename = "plot/" + "probe_" + mapkind
    make_plot(plot_filename, data, "present", "absent")


def plot_overwrite(filename, mapkind):
    data = read_csv(filename)
    plot_filename = "plot/" + "overwrite_" + mapkind
    make_plot(plot_filename, data, "probes", "skip")


if not(os.path.exists('plot')):
    os.mkdir('plot')

for file in os.listdir("out"):
    filename = os.fsdecode(file)
    if filename.endswith(".csv"):
        (test, mapkind) = parse_filename(filename)
        plot(filename, test, mapkind)
