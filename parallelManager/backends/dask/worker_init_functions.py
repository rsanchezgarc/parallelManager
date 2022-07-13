from distributed.protocol import dask_serialize, serialize


def patch_tensor_serialization():
    import torch
    @dask_serialize.register(torch.Tensor)
    def serialize_torch_Tensor(t):
        # with open("/home/sanchezg/tmp/debug_dask/tensor.txt", "w") as f:
        #     f.write(str(t)+"\n"+str(t.shape))

        device = t.device
        requires_grad_ = t.requires_grad

        t = t.cpu()
        if requires_grad_:
            sub_header, frames = serialize(t.detach().numpy())
        else:
            sub_header, frames = serialize(t.numpy())

        header = {"sub-header": sub_header}
        if t.grad is not None:
            grad_header, grad_frames = serialize(t.grad.numpy())
            header["grad"] = {"header": grad_header, "start": len(frames)}
            frames += grad_frames
        header["requires_grad"] = requires_grad_
        header["device"] = device.type
        return header, frames
