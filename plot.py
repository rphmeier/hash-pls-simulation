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

            data["b_mean"][(load_factor, meta_bits)] = float(row[3])
            data["b_50"][(load_factor, meta_bits)] = float(row[4])
            data["b_95"][(load_factor, meta_bits)] = float(row[5])
            data["b_99"][(load_factor, meta_bits)] = float(row[6])

    return data

def make_plot(plot_filename, csv_data):
    load_factors = sorted(list(set(x[0] for x in csv_data)))
    meta_bit_counts = sorted(list(set(x[1] for x in csv_data)))
    
    fig, ax = plt.subplots()
    ax.set(xlabel="load factor", ylabel="operations")
    ax.set_yscale('log')

    for meta_bits in meta_bit_counts:
        data = [csv_data[(load_factor, meta_bits)] for load_factor in load_factors]
        plt.plot(load_factors, data, label=f"{meta_bits} meta bits")
        
    plt.legend()
    plt.savefig(plot_filename)
    plt.close(fig)

def make_plots(filename, op_name, mapkind, a_name, b_name):
    data = read_csv(filename)

    if not(os.path.exists(f"plot/{mapkind}")):
        os.mkdir(f"plot/{mapkind}")

    make_plot(f"plot/{mapkind}/{op_name}_{a_name}_mean", data["a_mean"]);
    make_plot(f"plot/{mapkind}/{op_name}_{a_name}_50", data["a_50"]);
    make_plot(f"plot/{mapkind}/{op_name}_{a_name}_95", data["a_95"]);
    make_plot(f"plot/{mapkind}/{op_name}_{a_name}_99", data["a_99"]);

    make_plot(f"plot/{mapkind}/{op_name}_{b_name}_mean", data["b_mean"]);
    make_plot(f"plot/{mapkind}/{op_name}_{b_name}_50", data["b_50"]);
    make_plot(f"plot/{mapkind}/{op_name}_{b_name}_95", data["b_95"]);
    make_plot(f"plot/{mapkind}/{op_name}_{b_name}_99", data["b_99"]);

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
