<?php


function makeMessage($bucket, $message, $messageKey = null)
{
    return chr(strlen($bucket)) . $bucket . chr(strlen($messageKey ?? "")) . $messageKey . $message;
}
$msg = makeMessage("abc", "\x01\x02\x03", "abc");
echo bin2hex(pack("V", strlen($msg)) . $msg) . "\n";
die;
$sock = stream_socket_client("unix:///tmp/kahego.sock", $errno, $errstr);
if (!$sock) {
    die("Failed to connect to the Unix socket: $errstr ($errno)");
}
for ($i = 0; $i <= 500000; $i++) {
    if ($i % 1000 == 0) usleep(100000); //10000msg/s
    $msg = makeMessage("requests", "messageToBeSent{$i}_" . bin2hex(random_bytes(4096)));
    fwrite($sock, pack("V", strlen($msg)) . $msg);
}
fflush($sock);
fclose($sock);
