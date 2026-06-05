"""OpenSAI 数据面：设备间大块数据(产物/特征张量)直传，基于 QUIC。

控制面（server:8888 TCP 星型）负责编排；本模块负责数据面：
  - ArtifactServer：每台 device 跑一个，把本机产出的 artifact 提供给其它 device 拉取。
  - fetch_blob：从某台 device 拉取一个 artifact 到本地文件。

单路基础版（Phase 2a）。后续可在此之上扩展多网卡多路径(MPQUIC)。
"""
