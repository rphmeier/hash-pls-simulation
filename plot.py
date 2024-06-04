import os
import csv
import matplotlib.pyplot as plt
import numpy

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

def blank_csv_data():
    csv_data = {}

    csv_data["a_mean"] = {}
    csv_data["a_50"] = {}
    csv_data["a_95"] = {}
    csv_data["a_99"] = {}

    csv_data["b_mean"] = {}
    csv_data["b_50"] = {}
    csv_data["b_95"] = {}
    csv_data["b_99"] = {}

    return csv_data

def read_csv(filename):
    data = blank_csv_data()
    with open("out/" + filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            load_factor = float(row[0])
            # TODO size = row[1]
            meta_bits = int(row[2])

            data["a_mean"][(load_factor, meta_bits)] = float(row[3])
            data["a_50"][(load_factor, meta_bits)] = float(row[4])
            data["a_95"][(load_factor, meta_bits)] = float(row[5])
            data["a_99"][(load_factor, meta_bits)] = float(row[6])

            data["b_mean"][(load_factor, meta_bits)] = float(row[7])
            data["b_50"][(load_factor, meta_bits)] = float(row[8])
            data["b_95"][(load_factor, meta_bits)] = float(row[9])
            data["b_99"][(load_factor, meta_bits)] = float(row[10])

    return data

def make_plot(plot_filename, csv_data, plot_names):
    fig, ax = plt.subplots(int(len(plot_names) / 2), figsize=(8, 8), ncols=2)
    fig.suptitle("operations at load factor")
    fig.supxlabel("load factor")

    for (i, (plot_name, data_name)) in zip(numpy.ndindex(ax.shape), plot_names):
        plot_data = csv_data[data_name]

        meta_bit_counts = sorted(list(set(x[1] for x in plot_data)))
    
        ax[i].set(ylabel="operations")
        ax[i].set_yscale('log')
        ax[i].set_title(plot_name)

        for meta_bits in meta_bit_counts:
            load_factors = set()
            for (l, b) in plot_data:
                if b == meta_bits:
                    load_factors.add(l)

            load_factors = sorted(list(load_factors))
            data = [plot_data[(load_factor, meta_bits)] for load_factor in load_factors]
            ax[i].plot(load_factors, data, label=f"{meta_bits} meta bits" if i == (0, 0) else "")
        
    plt.figlegend()
    plt.savefig(plot_filename)
    plt.close(fig)

def make_plots(filename, op_name, mapkind, a_name, b_name):
    data = read_csv(filename)

    if not(os.path.exists(f"plot/{mapkind}")):
        os.mkdir(f"plot/{mapkind}")

    make_plot(
        f"plot/{mapkind}_{op_name}_{a_name}", 
        data,
        [
            (f"mean {a_name}", "a_mean"), 
            (f"50th percentile {a_name}", "a_50"), 
            (f"95th percentile {a_name}", "a_95"), 
            (f"99th percentile {a_name}", "a_99"),
        ]
    )

    make_plot(
        f"plot/{mapkind}_{op_name}_{b_name}", 
        data,
        [
            (f"mean {b_name}", "b_mean"), 
            (f"50th percentile {b_name}", "b_50"), 
            (f"95th percentile {b_name}", "b_95"), 
            (f"99th percentile {b_name}", "b_99"),
        ]
    )

def plot_grow(filename, mapkind):
    make_plots(filename, "grow", mapkind, "probes", "writes")

def plot_churn(filename, mapkind):
    make_plots(filename, "churn", mapkind, "probes", "writes")

def plot_probe(filename, mapkind):
    make_plots(filename, "probe", mapkind, "present", "absent")


if not(os.path.exists('plot')):
    os.mkdir('plot')

for file in os.listdir("out"):
    filename = os.fsdecode(file)
    if filename.endswith(".csv"):
        (test, mapkind) = parse_filename(filename)
        plot(filename, test, mapkind)
