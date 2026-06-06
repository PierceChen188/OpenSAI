"""Briefly train the collaborative model (shared backbone + fusion head) on CIFAR-10
and save weights, so the Algorithm_FeatureExtract / Algorithm_Fusion demo shows
above-random accuracy. Small model + subset + few epochs => fast on CPU.

Run:  .venv/Scripts/python.exe scripts/train_collab.py
"""
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, 'Offloading-System'))

import torch
import torch.nn as nn
import torchvision
import torchvision.transforms as T
from torch.utils.data import DataLoader, Subset

from device.implements.collab_model import (Backbone, FusionHead, data_dir,
                                            BACKBONE_PATH, FUSION_PATH, HALF_W)

TRAIN_N = 12000   # subset for speed
EPOCHS = 5
BATCH = 256


def halves(imgs):
    return imgs[:, :, :, :HALF_W], imgs[:, :, :, HALF_W:]


def main():
    torch.manual_seed(0)
    root = data_dir()
    print(f"[train] CIFAR-10 root = {root}", flush=True)
    train = torchvision.datasets.CIFAR10(root=root, train=True, download=False, transform=T.ToTensor())
    dl = DataLoader(Subset(train, range(min(TRAIN_N, len(train)))), batch_size=BATCH, shuffle=True)

    backbone, fusion = Backbone(), FusionHead()
    opt = torch.optim.Adam(list(backbone.parameters()) + list(fusion.parameters()), lr=1e-3)
    crit = nn.CrossEntropyLoss()

    for ep in range(EPOCHS):
        backbone.train(); fusion.train()
        t0 = time.time(); tot = correct = 0; lsum = 0.0
        for imgs, labels in dl:
            left, right = halves(imgs)
            logits = fusion(torch.cat([backbone(left), backbone(right)], dim=1))
            loss = crit(logits, labels)
            opt.zero_grad(); loss.backward(); opt.step()
            lsum += loss.item() * labels.size(0); tot += labels.size(0)
            correct += (logits.argmax(1) == labels).sum().item()
        print(f"[train] epoch {ep + 1}/{EPOCHS}  loss={lsum / tot:.3f}  "
              f"train_acc={correct / tot:.3f}  ({time.time() - t0:.1f}s)", flush=True)

    torch.save(backbone.state_dict(), BACKBONE_PATH)
    torch.save(fusion.state_dict(), FUSION_PATH)
    print(f"[train] saved {os.path.basename(BACKBONE_PATH)} + {os.path.basename(FUSION_PATH)}", flush=True)

    # quick held-out accuracy
    test = torchvision.datasets.CIFAR10(root=root, train=False, download=False, transform=T.ToTensor())
    tdl = DataLoader(Subset(test, range(2000)), batch_size=BATCH)
    backbone.eval(); fusion.eval(); c = t = 0
    with torch.no_grad():
        for imgs, labels in tdl:
            left, right = halves(imgs)
            logits = fusion(torch.cat([backbone(left), backbone(right)], dim=1))
            c += (logits.argmax(1) == labels).sum().item(); t += labels.size(0)
    print(f"[train] held-out test_acc (2000 imgs) = {c / t:.3f}  (random = 0.10)", flush=True)


if __name__ == '__main__':
    main()
