import re
import matplotlib.pyplot as plt

def parse_data(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Parsing the training parameters
    training_parameters = re.findall(r'(\w+)\s*=\s*(\S+)', content)

    # Parsing the epoch, training loss, and validation loss data
    pattern = r'Epoch \[(\d+)/\d+\] Training Loss: (\d+\.\d+) Validation Loss: (\d+\.\d+)'
    data = re.findall(pattern, content)

    return training_parameters, data

def plot_data(training_parameters, data):
    # Converting the extracted data to a suitable format for plotting
    epochs, training_loss, validation_loss = zip(*[(int(epoch), float(train_loss), float(val_loss)) for epoch, train_loss, val_loss in data])

    # Plotting the data
    plt.figure(figsize=(12, 6))
    plt.plot(epochs, training_loss, label='Training Loss', color='blue')
    plt.plot(epochs, validation_loss, label='Validation Loss', color='red')
    plt.xlabel('Epoch')
    plt.ylabel('Loss')
    plt.title('Training and Validation Loss per Epoch')
    plt.legend()

    # Adding the training parameters as an annotation
    param_text = "\n".join([f"{param}: {value}" for param, value in training_parameters])
    plt.annotate(param_text, xy=(0.02, 0.75), xycoords='axes fraction', bbox=dict(boxstyle="round,pad=0.3", edgecolor="black", facecolor="white"))

    plt.show()

def main():
    file_path = './models/training.txt'  # Replace with the path to your file
    training_parameters, data = parse_data(file_path)
    plot_data(training_parameters, data)

if __name__ == "__main__":
    main()
