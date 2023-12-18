import torch
import torch.nn as nn
import torch.optim as optim
import torch_geometric.nn as geom_nn
from torch.utils.data import Dataset, DataLoader, random_split

# Define the Temporal Transformer
class TemporalTransformer(nn.Module):
    def __init__(self, num_nodes, sequence_length, num_features, num_out_features, n_heads):
        super(TemporalTransformer, self).__init__()
        self.transformers = nn.ModuleList([
            nn.TransformerEncoder(
                nn.TransformerEncoderLayer(
                    d_model=sequence_length,
                    nhead=n_heads,
                    dim_feedforward=sequence_length
                ),
                num_layers = 2
            ) for _ in range(num_nodes)
        ])
        self.fc = nn.Linear(sequence_length * num_features, num_out_features)

    def forward(self, x):
        # x shape: [batch_size, num_nodes, sequence_length]
        batch_size, num_nodes, seq_len = x.shape
        # x = x.view(batch_size * num_nodes, seq_len, num_features)
        x = x.permute(0, 2, 1)
        x = torch.cat([self.fc(self.transformers[node](x[:,:,node])) for node in range(num_nodes)], dim=0)
        x = x.view(batch_size, num_nodes, num_out_features)
        return x

# Define the SpatioTemporal Model
class SpatioTemporalModel(nn.Module):
    def __init__(self, num_nodes, sequence_length, num_features, num_out_features, n_heads, gnn_output_dim):
        super(SpatioTemporalModel, self).__init__()
        self.temporal = TemporalTransformer(num_nodes, sequence_length, num_features, num_out_features, n_heads)
        self.spatial = geom_nn.GATConv(num_out_features, gnn_output_dim)
        self.predictor = nn.Linear(gnn_output_dim, 2)  # Predicting next 2 days

    def forward(self, x, edge_index):
        x = self.temporal(x)  # Output shape: [batch_size, num_nodes, num_out_features]
        batch_size = x.size(0)
        predictions = []
        # print(x.shape)
        #print(x[1].shape)
        for i in range(batch_size):
            spatial_out = self.spatial(x[i], edge_index)  # Process each batch separately
            prediction = self.predictor(spatial_out)  # Shape: [num_nodes, 2]
            predictions.append(prediction)
        return torch.stack(predictions)  # Shape: [batch_size, num_nodes, 2]

# Custom Dataset class for windowed traffic data
class TrafficDataset(Dataset):
    def __init__(self, data, window_size, prediction_size):
        self.data = data
        self.window_size = window_size
        self.prediction_size = prediction_size
        self.num_nodes, self.num_days = data.shape

    def __len__(self):
        return self.num_days - self.window_size - self.prediction_size + 1

    def __getitem__(self, idx):
        x = self.data[:, idx:idx + self.window_size]
        y = self.data[:, idx + self.window_size:idx + self.window_size + self.prediction_size]
        return x, y

# Generating random traffic data for testing



