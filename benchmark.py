import argparse

from src.utility.Config import load_config
from src.utility.run_refinery import run_command


def main():
    args = parse_arguments()
    config = load_config(args.config)

    full_benchmark_result = []
    full_benchmark_result_headers = ""

    node_min = args.node_min_multi_start
    while node_min <= args.node_min_multi_end:
        node_max = args.node_max_multi_start
        while node_max <= args.node_max_multi_end:
            for repetition in range(args.repeat):
                print(f"node_min: {node_min}, node_max: {node_max}, repetition: {repetition+1}")
                command = f"python main.py -min {node_min} -max {node_max} -srr -ssr -db {args.db_name} -b"
                if args.additional_constraints:
                    command += f" -ac {args.additional_constraints}"
                run_command(command)

                with open(config.BENCHMARK_RESULT_PATH) as file:
                    lines = file.readlines()
                    if not full_benchmark_result_headers:
                        full_benchmark_result_headers = lines[0]
                    full_benchmark_result.append(lines[1])

            node_max += args.node_max_multi_step
        node_min += args.node_min_multi_step

    with open(config.FULL_BENCHMARK_RESULT_PATH, "w") as file:
        file.write(full_benchmark_result_headers)
        file.write("\n".join(full_benchmark_result))


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config",
                        default="config.json",
                        help="Path to a custom configuration JSON file")
    parser.add_argument("-db", "--db-name",
                        help="Name of the database to be generated")
    parser.add_argument("-min-start", "--node-min-multi-start",
                        default=10,
                        type=float,
                        help="Multiplier for the minimum node count in refinery. (default: 10)")
    parser.add_argument("-min-end", "--node-min-multi-end",
                        default=10,
                        type=float)
    parser.add_argument("-min-step", "--node-min-multi-step",
                        default=1,
                        type=float)
    parser.add_argument("-max-start", "--node-max-multi-start",
                        default=1.2,
                        type=float,
                        help="Multiplier for the maximum node count in refinery. max = min * max_multi. (default: 1.2)")
    parser.add_argument("-max-end", "--node-max-multi-end",
                        default=1.2,
                        type=float)
    parser.add_argument("-max-step", "--node-max-multi-step",
                        default=1,
                        type=float)
    parser.add_argument("-r", "--repeat",
                        default=1,
                        type=int)
    parser.add_argument("-ac", "--additional-constraints",
                        help="Path to a file, containing additional refinery constraints")

    return parser.parse_args()


main()
