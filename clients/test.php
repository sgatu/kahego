<?php

$sock = stream_socket_client("unix:///tmp/kahego.sock", $errno, $errstr);
if (!$sock) {
    die("Failed to connect to the Unix socket: $errstr ($errno)");
}
function makeMessage($bucket, $message, $messageKey = null)
{
    return chr(strlen($bucket)) . $bucket . chr(strlen($messageKey ?? "")) . $messageKey . $message;
}


for ($i = 0; $i <= 10000000; $i++) {
    if ($i % 1000 == 0) usleep(50000);
    $msg = makeMessage("requests", "messageToBeSent{$i}_" . bin2hex(random_bytes(4096)));
    fwrite($sock, pack("V", strlen($msg)) . $msg);
}
fflush($sock);
fclose($sock);
