<?php

$sock = stream_socket_client("unix:///tmp/kahego.sock", $errno, $errstr);
if (!$sock) {
    die("Failed to connect to the Unix socket: $errstr ($errno)");
}
function makeMessage($bucket, $message, $messageKey = null)
{
    return chr(strlen($bucket)) . $bucket . chr(strlen($messageKey ?? "")) . $messageKey . $message;
}

$msg = makeMessage("b01", "messageToBeSent");
for ($i = 0; $i < 5; $i++) {
    fwrite($sock, pack("V", strlen($msg)) . $msg);
}
fflush($sock);
fclose($sock);
