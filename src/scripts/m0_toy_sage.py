import torch
import torch.nn.functional as F
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import SAGEConv
from ogb.nodeproppred import PygNodePropPredDataset
from sklearn.metrics import accuracy_score
import torch.serialization
from torch_geometric.data.data import DataEdgeAttr, DataTensorAttr
from torch_geometric.data.storage import GlobalStorage
torch.serialization.add_safe_globals([DataEdgeAttr, DataTensorAttr, GlobalStorage])


device = "mps" if torch.backends.mps.is_available() else "cpu"


# data loading

dataset = PygNodePropPredDataset(name="ogbn-arxiv", root="/tmp/ogb")
data = dataset[0]
data.y = data.y.squeeze()
from torch_geometric.transforms import ToUndirected
data = ToUndirected()(data)   # add reverse edges so messages flow both ways
split = dataset.get_idx_split()

print(data)
print("features per node: ", data.num_features)

train_loader = NeighborLoader(
    data,
    num_neighbors=[15, 10],
    batch_size=1024,
    input_nodes=split["train"],
    shuffle=True,
)
eval_loader = NeighborLoader(
    data,
    num_neighbors=[15,10],
    batch_size=4096,
    input_nodes=split['valid'],
)


# model
class SAGE(torch.nn.Module):
    def __init__(self, in_dim, hidden, out_dim):
        super().__init__()
        self.conv1 = SAGEConv(in_dim, hidden)
        self.conv2 = SAGEConv(hidden, out_dim)

    def forward(self, x, edge_index):
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.5, training=self.training)
        return self.conv2(x, edge_index)
    
model = SAGE(data.num_features, 128, dataset.num_classes).to(device)
opt = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)


def train_epoch():
    model.train()
    total = 0.0
    for batch in train_loader:
        batch = batch.to(device)
        opt.zero_grad()
        out = model(batch.x, batch.edge_index)
        y = batch.y[:batch.batch_size] # only compute loss on the target nodes in the batch
        loss = F.cross_entropy(out[:batch.batch_size],y)
        loss.backward()
        opt.step()
        total += float(loss) * batch.batch_size
    return total / split['train'].numel()

@torch.no_grad()
def evaluate():
    model.eval()
    preds, ys = [], []
    for batch in eval_loader:
        batch = batch.to(device)
        out = model(batch.x, batch.edge_index)[:batch.batch_size]
        preds.append(out.argmax(dim=-1).cpu())
        ys.append(batch.y[:batch.batch_size].cpu())
    return accuracy_score(torch.cat(ys), torch.cat(preds))


for epoch in range(1,11):
    loss = train_epoch()
    acc = evaluate()
    print(f"epoch {epoch:02d} | loss {loss:.4f} | val acc {acc:.4f}")
