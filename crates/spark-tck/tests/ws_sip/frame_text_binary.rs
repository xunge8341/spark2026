//! `frame_text_binary` 契约测试（集成测试版）。
//!
//! 通过在集成测试中嵌套模块，确保测试筛选指令
//! `cargo test -p spark-tck -- ws_sip::frame_text_binary::*`
//! 仍能定位到期望的命名空间。

mod ws_sip {
    pub mod frame_text_binary {
        use core::str;

        use spark_codec_sip::{
            parse_request,
            ws::frame_text_binary::{sip_to_ws, ws_to_sip, FrameKind, SipMessage, WsSipError},
        };
        use spark_core::buffer::BufView;

        /// 构造一个最小化的 WebSocket 帧字节序列。
        ///
        /// # 教案级说明
        /// - **Why**：测试需要自定义 opcode、FIN、掩码等组合场景；
        /// - **How**：按 RFC 6455 §5.2 拼接头部/扩展长度/掩码；
        /// - **Contract**：`payload` 为未掩码前的原始字节。
        fn build_frame(fin: bool, opcode: u8, payload: &[u8], mask: Option<[u8; 4]>) -> Vec<u8> {
            let mut frame = Vec::new();
            frame.push(if fin { 0x80 } else { 0x00 } | (opcode & 0x0F));
            let mask_bit = if mask.is_some() { 0x80 } else { 0x00 };
            match payload.len() {
                len if len <= 125 => {
                    frame.push(mask_bit | (len as u8));
                }
                len if len <= u16::MAX as usize => {
                    frame.push(mask_bit | 126);
                    frame.extend_from_slice(&(len as u16).to_be_bytes());
                }
                len => {
                    frame.push(mask_bit | 127);
                    frame.extend_from_slice(&(len as u64).to_be_bytes());
                }
            }

            if let Some(key) = mask {
                frame.extend_from_slice(&key);
                for (idx, byte) in payload.iter().enumerate() {
                    frame.push(byte ^ key[idx % 4]);
                }
            } else {
                frame.extend_from_slice(payload);
            }

            frame
        }

        /// 将帧缓冲转换为 `BufView` 切片，便于调用 `ws_to_sip`。
        fn to_buf_views<'a>(frames: &'a [Vec<u8>]) -> Vec<&'a dyn BufView> {
            frames
                .iter()
                .map(|frame| frame.as_slice() as &'a dyn BufView)
                .collect()
        }

        #[test]
        fn aggregate_single_text_frame() {
            let sip_text =
                "INVITE sip:bob@example.com SIP/2.0\r\nVia: SIP/2.0/WS example.com\r\n\r\n";
            let frame = build_frame(true, 0x1, sip_text.as_bytes(), None);
            let frames = vec![frame];
            let views = to_buf_views(&frames);

            let messages = ws_to_sip(&views).expect("单帧文本应成功解析");
            assert_eq!(messages.len(), 1);
            let message = &messages[0];
            assert_eq!(message.frame_kind(), FrameKind::Text);
            assert_eq!(message.payload(), sip_text.as_bytes());

            let text = str::from_utf8(message.payload()).expect("payload 应为 UTF-8");
            let parsed = parse_request(text).expect("SIP 报文应可解析");
            assert!(matches!(
                parsed.start_line,
                spark_codec_sip::StartLine::Request(_)
            ));
        }

        #[test]
        fn aggregate_fragmented_sequence() {
            let sip_text = "REGISTER sip:example.com SIP/2.0\r\nCall-ID: abc\r\n\r\n";
            let split = sip_text.as_bytes().len() / 2;
            let first = build_frame(false, 0x1, &sip_text.as_bytes()[..split], None);
            let second = build_frame(true, 0x0, &sip_text.as_bytes()[split..], None);
            let frames = vec![first, second];
            let views = to_buf_views(&frames);

            let messages = ws_to_sip(&views).expect("分片文本应聚合成功");
            assert_eq!(messages.len(), 1);
            let message = &messages[0];
            assert_eq!(message.frame_kind(), FrameKind::Text);
            assert_eq!(message.payload(), sip_text.as_bytes());
        }

        #[test]
        fn aggregate_masked_binary_frame() {
            let sip_text = "OPTIONS sip:gw@example.com SIP/2.0\r\nMax-Forwards: 70\r\n\r\n";
            let mask = [0xA5, 0x5A, 0x3C, 0xC3];
            let frame = build_frame(true, 0x2, sip_text.as_bytes(), Some(mask));
            let frames = vec![frame];
            let views = to_buf_views(&frames);

            let messages = ws_to_sip(&views).expect("掩码二进制帧应被兼容");
            assert_eq!(messages.len(), 1);
            let message = &messages[0];
            assert_eq!(message.frame_kind(), FrameKind::Binary);
            assert_eq!(message.payload(), sip_text.as_bytes());
        }

        #[test]
        fn sip_to_ws_roundtrip() {
            let sip_text = "BYE sip:bob@example.com SIP/2.0\r\nCall-ID: bye\r\n\r\n";
            let message = SipMessage::text(sip_text.as_bytes().to_vec()).expect("文本构造应成功");

            let mut frames = Vec::new();
            sip_to_ws(&message, &mut frames);
            assert_eq!(frames.len(), 1, "默认策略应输出单帧");

            let views = to_buf_views(&frames);
            let decoded = ws_to_sip(&views).expect("回环解析应成功");
            assert_eq!(decoded, vec![message]);
        }

        #[test]
        fn reject_unexpected_continuation() {
            let continuation = build_frame(true, 0x0, b"partial", None);
            let frames = vec![continuation];
            let views = to_buf_views(&frames);

            let error = ws_to_sip(&views).expect_err("孤立 continuation 应失败");
            assert!(matches!(error, WsSipError::UnexpectedContinuation));
        }

        #[test]
        fn reject_dangling_fragment() {
            let dangling = build_frame(false, 0x1, b"INVITE ", None);
            let frames = vec![dangling];
            let views = to_buf_views(&frames);

            let error = ws_to_sip(&views).expect_err("缺少 FIN 应失败");
            assert!(matches!(error, WsSipError::DanglingFragment));
        }
    }
}
