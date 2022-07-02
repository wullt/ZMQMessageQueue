import threading
import zmq
import os
import pathlib
from typing import List, Optional, Callable, Union  # pylint: disable=unused-import
import copy
import contextlib
import logging
import json


# some code from:
# https://github.com/Parquery/persizmq


class MessageStore:
    def __init__(self, save_dir):
        if isinstance(save_dir, str):
            save_dir = pathlib.Path(save_dir)
        elif not isinstance(save_dir, pathlib.Path):
            raise TypeError("save_dir must be a string or pathlib.Path")
        self._persistent_dir = save_dir
        self._persistent_dir.mkdir(parents=True, exist_ok=True)
        self._counter = 0
        self._paths = []
        self._first = None
        self._lock = threading.Lock()
        files = sorted(list(self._persistent_dir.iterdir()))
        for path in files:
            if path.suffix == ".json":
                self._paths.append(path)
            elif path.suffix == ".tmp":
                path.unlink()
        if self._paths:
            stem = self._paths[-1].stem
            value_err = None
            try:
                self._counter = int(stem) + 1
            except ValueError as e:
                value_err = e
            if value_err:
                raise ValueError(
                    "Invalid filename: {} in directory {}".format(stem, self._paths[-1])
                )
            pth = self._paths[0]
            self._first = pth.read_bytes()
        logging.info("MessageStore initialized")
        logging.info(
            "\tMessages are stored in: {}".format(self._persistent_dir.absolute())
        )
        logging.info("\tMessages in queue: {}".format(len(self._paths)))

    def front(self) -> Optional[bytes]:
        """
        makes a copy of the first pending message, but does not remove it from the persistent storage's
        internal queue.
        :return: copy of the first message, or None if no message in the queue
        """
        with self._lock:  # pylint: disable=not-context-manager
            if self._first is None:
                return None

            msg = copy.deepcopy(self._first)
            return msg

    def remove_first(self):
        """
        removes the first message from persistent storage's internal queue and from the disk.
        """
        with self._lock:
            if self._first is None:
                return None
            else:
                pth = self._paths.pop(0)
                pth.unlink()
                logging.info("Removed message #{} from queue".format(int(pth.stem)))
                # os.remove(pth)
            if not self._paths:
                self._first = None
            else:
                pth = self._paths[0]
                self._first = pth.read_bytes()
            return True

    def add_message(self, msg: Optional[bytes]) -> None:
        """
        adds a message to the persistent storage's internal queue.
        :param msg: message to be added
        """
        if msg is None:
            return

        with self._lock:  # pylint: disable=not-context-manager

            # Make sure the files can be sorted as strings (which breaks if you have files=[3.json, 21.json])
            pth = self._persistent_dir / "{:030d}.json".format(self._counter)
            tmp_pth = pth.parent / (pth.name + ".tmp")  # type: Optional[pathlib.Path]

            try:
                assert (
                    tmp_pth is not None
                ), "Unexpected tmp_pth None; expected it to be initialized just before."
                tmp_pth.write_bytes(msg)
                tmp_pth.rename(pth)
                tmp_pth = None

                self._paths.append(pth)
                self._counter += 1
                logging.info("Stored message #{} in queue".format(int(pth.stem)))

                if self._first is None:
                    self._first = msg

            finally:
                if tmp_pth is not None and tmp_pth.exists():  # type: ignore
                    tmp_pth.unlink()  # type: ignore


class ThreadedSubscriber:
    def __init__(
        self,
        subscriber: zmq.Socket,
        callback: Callable[[bytes], None],
        on_exception: Callable[[Exception], None],
    ) -> None:
        """
        :param subscriber: zeromq subscriber socket; only operated by ThreadedSubscriber, do not share among threads!
        :param callback:
            This function is called every time a message is received. Can be accessed and changed later
            through ThreadedSubscriber.callback
        :param on_exception: Is called when an exception occurs during the callback call.
        """

        if isinstance(subscriber, zmq.Socket):
            self._subscriber = subscriber
        else:
            raise TypeError(
                "unexpected type of the argument socket: {}".format(
                    subscriber.__class__.__name__
                )
            )

        self.callback = callback
        self.on_expection = on_exception
        self.operational = False

        self._exit_stack = contextlib.ExitStack()

        init_err = None  # type: Optional[Exception]

        try:
            # control structures
            self._context = zmq.Context()
            self._exit_stack.push(self._context)

            self._ctl_url = "inproc://ctl"
            self._publisher_ctl = self._context.socket(
                zmq.PUB
            )  # pylint: disable=no-member
            self._exit_stack.push(self._publisher_ctl)
            self._publisher_ctl.bind(self._ctl_url)

            self._subscriber_ctl = self._context.socket(
                zmq.SUB
            )  # pylint: disable=no-member
            self._exit_stack.push(self._subscriber_ctl)

            # thread
            self._thread = threading.Thread(target=self._listen)
            self._thread.start()
            self.operational = True

        except Exception as err:  # pylint: disable=broad-except
            init_err = err

        if init_err is not None:
            self._exit_stack.close()
            raise init_err  # pylint: disable=raising-bad-type

    def _listen(self) -> None:
        """
        listens on the zeromq subscriber. This function is expected to run in a separate thread.
        :return:
        """
        with self._context.socket(
            zmq.SUB
        ) as subscriber_ctl:  # pylint: disable=no-member
            subscriber_ctl.connect(self._ctl_url)
            subscriber_ctl.setsockopt_string(
                zmq.SUBSCRIBE, ""
            )  # pylint: disable=no-member

            poller = zmq.Poller()
            poller.register(subscriber_ctl, zmq.POLLIN)  # pylint: disable=no-member
            poller.register(self._subscriber, zmq.POLLIN)  # pylint: disable=no-member
            while True:
                try:
                    socks = dict(poller.poll())

                    if subscriber_ctl in socks and socks[subscriber_ctl] == zmq.POLLIN:
                        # received an exit signal
                        _ = subscriber_ctl.recv()
                        break

                    if (
                        self._subscriber in socks
                        and socks[self._subscriber] == zmq.POLLIN
                    ):
                        msg = self._subscriber.recv()
                        self.callback(msg)
                except Exception as err:  # pylint: disable=broad-except
                    self.on_expection(err)
                    break

    def shutdown(self) -> None:
        logging.info("Shutting down ThreadedSubscriber")
        """
        shuts down the threaded subscriber.
        :return:
        """
        if self.operational:
            self._publisher_ctl.send(
                data=b""
            )  # send a shutdown signal to the subscriber_ctl
            self._thread.join()
            self._exit_stack.close()

        self.operational = False

    def __enter__(self) -> "ThreadedSubscriber":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.operational:
            self.shutdown()
