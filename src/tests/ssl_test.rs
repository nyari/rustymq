pub use super::super::*;

use super::common::*;

use openssl::ssl::{SslMethod, SslConnector, SslAcceptor, SslVerifyMode};
use openssl;

use core::socket::{Socket, OpFlag, OutwardSocket, InwardSocket, BidirectionalSocket};
use core::transport::NetworkAddress;
use core::message::{TypedMessage, Message};
use std::sync::{Arc};

fn get_private_key() -> openssl::pkey::PKey<openssl::pkey::Private> {
    let key_string =
r"-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEApPsh1kuBn+n0Pfz8lEU0zls/SMlWsxCdLbZU+7KUXV+JQjWH
UVwwrp8QJEb4d4Nu/XFoDSBUiqTT5EUPcM2pi/PKlWacd9Pn7ejbs1QqyZyx5Im7
wb7ZhCJkznH/xGL5TRjQ7smcFuY+9S4lM2QXfY8JPr/OdhY6eiV/hHlZ/KkaZ0Hx
0XbmlVe6tp6QPUBF/FPGlL8C5uKNjX4hLa6m5w68fGWJTSNEgAv6WjN3wOnjmfQs
uKx7ruY604+OtyTjm3w+GrOXt6FgN4ZAdVCv8eXjA8CQvbh37uUJEPLiDjH7Po0M
2BsUI8mJvR7kXuV7VEoN9YBDRzasGkeVbvvfRQIDAQABAoIBACZay+8J9sS6mzGe
EwpVXIVs6TV+uU0/24q5DODHD371qu9dimv7xsWR7SrB5RnD0JXTO6FxlrX0w94Z
wBE1fukucDZzeITTGuRQcmMqehgM+b5r1t6gA+mYJV9pWVDKwbKOxmP9MvCP7qsJ
Y1SjPGLQUhHofZMub8Hd3dtqauU1+0/iR+JV95lW+VPiY6rHHP8bQmhjJGxobbPe
mdZ3b26HZja5UaaKF7Rk8sfd6T2poDBFB+I0Hm7d9G3u5PqnQN/AlKrUlhNkFaiG
jarCMbswYQ8LTl0BKJcQgbCt1srGKEcMTKNTgzkD6FsqlEtLKyBJR/GqfVMxhIq1
GJ0jfLECgYEA2z830eCbShZO1L0oPMAMwH0TQCIv7noK2dn+0LB8n+HjIxEYlW3g
wjhe5JEaXs2VkuuBINeCjjaCk+S63ySKEgOakUdFLYY004FflTCB+tQScD+aoc6J
46uG03xMK2Nj3yG7Koifn9jYh2mykf/Z31HR53Plp7ME8ObHoQKB+jcCgYEAwKMj
3quZ6lYPpENUddA5pCylyLjEOopVR9NIPcl03mSPU0jDTUNFfuCzWXOYpFmBpnnN
1/CN/dG0Ljod0+gogPV6xUzKRk3AAYLMtMpaw2ipXwQxRJ7xMLj3O2hvefeKyDcW
x12pLlGtTmwbV/xBvUmR4z9cqGM32gvi1LsKxGMCgYBIYzkE3ImpDnB8oO+WDzqm
myUt+Zuluzm179nIAV1EVIpv24coXxzkQ0RhZt80CeCmn4cE1uLOHYVDWzOv1RqZ
FAOGj+dxPmxWoNJ0KY3gyQBFe1qMreqs9scPMGzdrnUdCMAJLQ628hubqfRBbB39
M8CAEK30jpDFEQ08Rd2wRQKBgCpEoZ4+MOuGLrBwRZwEMGGhmk7Mm+HscIHuDi/g
gFA76Gbx1Eijth/81d2Oy7NkIFqS52O2WLGUzeBGyDyy+BAzzNh13PxIGxU5ygjx
TbEKyf8bQGQ6K9nw++6BH9S3SDBeRhVAq9qJ+Wj3t5g6tYH/Ho+qW35nJt4lNYRP
9jDZAoGAJ8FaGrlgjiBfzAmlWux0Hn1SN+JbrihcYEa92ELZ+z0+TKqmbzIMZLL5
GpSFLiDHiX7Fvf5mmUeLa1OIuQWfyp8JREYWNa7yiksCxXcXEdl1IhE+RpzjVoiS
XrKwBiQVP2H8m+SYNjlJxzcL8DV7kxJqoIOR5tQa4aNZhFW3XN4=
-----END RSA PRIVATE KEY-----";

    openssl::pkey::PKey::private_key_from_pem(key_string.as_bytes()).unwrap()
}

fn get_certificate() -> openssl::x509::X509 {
    let cert_string =
r"-----BEGIN CERTIFICATE-----
MIIDDTCCAfUCFD4fsnY8ZI6yp1MMVgQtfk+nGLbrMA0GCSqGSIb3DQEBCwUAMEIx
CzAJBgNVBAYTAkhVMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0Rl
ZmF1bHQgQ29tcGFueSBMdGQwIBcNMjAxMTI5MTU1NTEzWhgPMjI5NDA5MTMxNTU1
MTNaMEIxCzAJBgNVBAYTAkhVMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNV
BAoME0RlZmF1bHQgQ29tcGFueSBMdGQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw
ggEKAoIBAQCk+yHWS4Gf6fQ9/PyURTTOWz9IyVazEJ0ttlT7spRdX4lCNYdRXDCu
nxAkRvh3g279cWgNIFSKpNPkRQ9wzamL88qVZpx30+ft6NuzVCrJnLHkibvBvtmE
ImTOcf/EYvlNGNDuyZwW5j71LiUzZBd9jwk+v852Fjp6JX+EeVn8qRpnQfHRduaV
V7q2npA9QEX8U8aUvwLm4o2NfiEtrqbnDrx8ZYlNI0SAC/paM3fA6eOZ9Cy4rHuu
5jrTj463JOObfD4as5e3oWA3hkB1UK/x5eMDwJC9uHfu5QkQ8uIOMfs+jQzYGxQj
yYm9HuRe5XtUSg31gENHNqwaR5Vu+99FAgMBAAEwDQYJKoZIhvcNAQELBQADggEB
ABJ5/ieDH4ODU63jzRekMi1idJS6BBBsPT2+mYvv36mTCOouEqeSzKqFxHhJnLus
3fDBo2GDi9xpVrhIDbj00O/Onet3eFv4qcIM3UCMjBFJN3hybaDKmM/MOJgGm23X
h0/mvrRHCvm8Can1OAu4bmi+D/zhVLTXG2sp/+pH4HS8LCujbvxyoU57YzVktny3
WDYRmXzH3GI6Jk5RNIVLl/v6B/iIFM2LbHCStC4Q0cc8cAxu+unS8mPzhwJBHzae
v4OzSoq20+a2uaIpUDOFs30eRpK6zl6e2OI2cHrsFkzOM/Vux6hf89cuykkFr+54
PJT+e7IFIhf2fUJQOB++xmo=
-----END CERTIFICATE-----";
    openssl::x509::X509::from_pem(cert_string.as_bytes()).unwrap()
}

#[test]
fn simple_req_rep_ssl_test() {
    let mut requestor = model::reqrep::RequestSocket::new(transport::network::ssl::InitiatorTransport::new(transport::network::ssl::StreamConnectionBuilder::new({let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
                                                                                                                                                                  builder.set_verify(SslVerifyMode::NONE);
                                                                                                                                                                  builder}.build())));
    let mut replier = model::reqrep::ReplySocket::new(transport::network::ssl::AcceptorTransport::new(transport::network::ssl::StreamConnectionBuilder::new({let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
                                                                                                                                                             builder.set_verify(SslVerifyMode::NONE);
                                                                                                                                                             builder}.build()),
                                                                                                      transport::network::ssl::StreamListenerBuilder::new(Arc::new(|| {
                                                                                                         let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
                                                                                                         builder.set_private_key(get_private_key().as_ref()).unwrap();
                                                                                                         builder.set_certificate(get_certificate().as_ref()).unwrap();
                                                                                                         builder.set_verify(SslVerifyMode::NONE);
                                                                                                         builder.build()
                                                                                                      }))));

    replier.bind(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:45432".to_string()).unwrap())).unwrap();
    requestor.connect(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:45432".to_string()).unwrap())).unwrap();

    let base = TestingStruct{a: 5, b: 5};
    let message = TypedMessage::new(base);

    requestor.send_typed(message, OpFlag::NoWait).unwrap();
    replier.respond_typed(OpFlag::Wait, OpFlag::Wait, |rmessage:TypedMessage<TestingStruct>| {
        TypedMessage::new(rmessage.payload().clone()).continue_exchange_metadata(rmessage.into_metadata())
    }).unwrap();
    let final_message = requestor.receive_typed::<TestingStruct>(OpFlag::Wait).expect("Hello");

    assert_eq!(base, final_message.into_payload());
}
