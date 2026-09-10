use anyhow::Result;

pub trait Sender<T>: Clone {
    fn send(&mut self, t: T) -> impl Future<Output = Result<()>>;
}

pub trait Receiver<T> {
    fn recv(&mut self) -> impl Future<Output = Result<T>>;
}

pub trait MultiProducerSingleConsumer: 'static {
    type Sender<T>: Sender<T>;
    type Receiver<T>: Receiver<T>;
    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>);
}

pub type Mpsc = target::Mpsc;
pub type MpscReceiver<T> = target::MpscReceiver<T>;
pub type MpscSender<T> = target::MpscSender<T>;

mod target {
    use super::MultiProducerSingleConsumer;
    use super::Receiver;
    use super::Sender;
    use anyhow::Result;
    use futures::SinkExt;
    use futures::channel::mpsc::UnboundedReceiver;
    use futures::channel::mpsc::UnboundedSender;
    use futures::channel::mpsc::unbounded;

    pub struct Mpsc;

    impl MultiProducerSingleConsumer for Mpsc {
        type Receiver<T> = MpscReceiver<T>;
        type Sender<T> = MpscSender<T>;

        fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>) {
            let (tx, rx) = unbounded();
            (MpscSender::new(tx), MpscReceiver::new(rx))
        }
    }

    pub struct MpscReceiver<T>(UnboundedReceiver<T>);

    impl<T> Receiver<T> for MpscReceiver<T> {
        async fn recv(&mut self) -> Result<T> {
            Ok(self.0.recv().await?)
        }
    }

    impl<T> MpscReceiver<T> {
        fn new(t: UnboundedReceiver<T>) -> Self {
            Self(t)
        }
    }

    pub struct MpscSender<T>(UnboundedSender<T>);

    impl<T> Sender<T> for MpscSender<T> {
        async fn send(&mut self, t: T) -> Result<()> {
            Ok(self.0.send(t).await?)
        }
    }

    impl<T> Clone for MpscSender<T> {
        fn clone(&self) -> Self {
            MpscSender(self.0.clone())
        }
    }

    impl<T> MpscSender<T> {
        fn new(t: UnboundedSender<T>) -> Self {
            Self(t)
        }
    }
}
